/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */

#include "foedus/cache/cache_hashtable.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace cache {

BucketId determine_logical_buckets(BucketId physical_buckets) {
  ASSERT_ND(physical_buckets >= 1024U);
  // to speed up, leave space in neighbors of the last bucket.
  // Instead, we do not wrap-around.
  // Also, leave space for at least one cacheline so that we never overrun when we prefetch.
  BucketId buckets = physical_buckets - kHopNeighbors - 64ULL;

  // to make the division-hashing more effective, make it prime-like.
  BucketId logical_buckets = assorted::generate_almost_prime_below(buckets);
  ASSERT_ND(logical_buckets <= physical_buckets);
  ASSERT_ND((logical_buckets & (logical_buckets - 1U)) != 0);  // at least not power of 2
  return logical_buckets;
}

uint32_t determine_overflow_list_size(BucketId physical_buckets) {
  // there should be very few overflow entries. This can be super small.
  const uint32_t kOverflowFraction = 128U;
  const uint32_t kOverflowMinSize = 256U;
  uint32_t overflow_size = physical_buckets / kOverflowFraction;
  if (overflow_size <= kOverflowMinSize) {
    overflow_size = kOverflowMinSize;
  }
  return overflow_size;
}

HashFunc::HashFunc(BucketId physical_buckets)
  : logical_buckets_(determine_logical_buckets(physical_buckets)),
    physical_buckets_(physical_buckets),
    bucket_div_(logical_buckets_) {
}

CacheHashtable::CacheHashtable(BucketId physical_buckets, uint16_t numa_node)
  : numa_node_(numa_node),
  overflow_buckets_count_(determine_overflow_list_size(physical_buckets)),
  hash_func_(physical_buckets),
  clockhand_(0) {
  buckets_memory_.alloc(
    sizeof(CacheBucket) * physical_buckets,
    1U << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node);
  refcounts_memory_.alloc(
    sizeof(CacheRefCount) * physical_buckets,
    1U << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node);
  buckets_ = reinterpret_cast<CacheBucket*>(buckets_memory_.get_block());
  refcounts_ = reinterpret_cast<CacheRefCount*>(refcounts_memory_.get_block());

  // index-0 should be never used. 0 means null.
  buckets_[0].reset();
  refcounts_[0].count_ = 0;

  // for overflow list
  overflow_buckets_memory_.alloc(
    sizeof(CacheOverflowEntry) * overflow_buckets_count_,
    1U << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node);
  overflow_buckets_ = reinterpret_cast<CacheOverflowEntry*>(overflow_buckets_memory_.get_block());
  overflow_buckets_head_ = 0;
  // index-0 should be never used. 0 means null.
  overflow_buckets_[0].bucket_.reset();
  overflow_buckets_[0].next_ = 0;
  overflow_buckets_[0].padding_ = 0;
  overflow_buckets_[0].refcount_.count_ = 0;

  // initially all entries are in free list.
  overflow_free_buckets_head_ = 1;
  for (OverflowPointer i = 1U; i < overflow_buckets_count_; ++i) {
    if (i < overflow_buckets_count_ - 1U) {
      overflow_buckets_[i].next_ = i + 1U;
    } else {
      overflow_buckets_[i].next_ = 0;
    }
  }
}


ErrorCode CacheHashtable::install(storage::SnapshotPagePointer page_id, ContentId content) {
  ASSERT_ND(content != 0);

  // Grab a bucket to install a new page.
  // The bucket does not have to be the only bucket to serve the page, so
  // the logic below is much simpler than typical bufferpool.
  BucketId ideal_bucket = get_bucket_number(page_id);
  PageIdTag tag = HashFunc::get_tag(page_id);
  ASSERT_ND(tag != 0);

  CacheBucket new_bucket;
  new_bucket.reset(content, tag);

  // An opportunistic optimization. if the exact bucket already has the same page_id,
  // most likely someone else is trying to install it at the same time. let's wait.
  for (BucketId bucket = ideal_bucket; bucket < ideal_bucket + kHopNeighbors; ++bucket) {
    if (!buckets_[bucket].is_content_set()) {
      // looks like this is empty!
      buckets_[bucket] = new_bucket;  // 8-byte implicitly-atomic write
      refcounts_[bucket].count_ = 1;
      // this might be immediately overwritten by someone else, but that's fine.
      // that only causes a future cache miss. no correctness issue.
      return kErrorCodeOk;
    }
  }

  // unlucky, no empty slot. If this happens often, we are seriously troubled.
  DVLOG(0) << "Ohhh, we have to add this to overflow list! This should be really rare."
    << " page_id=" << assorted::Hex(page_id)
    << ", content=" << assorted::Hex(content)
    << ", ideal_bucket=" << assorted::Hex(ideal_bucket)
    << ", tag=" << assorted::Hex(tag)
    << ", page_id=" << assorted::Hex(page_id);

  // we come here anyway very occasionally, so taking mutex here wouldn't cause performance issue.
  // note that this mutex just protects the free-list, which is rarely used.
  soc::SharedMutexScope scope(&overflow_free_buckets_mutex_);
  OverflowPointer new_overflow_entry = overflow_free_buckets_head_;
  if (new_overflow_entry == 0) {
    LOG(ERROR) << "Oh my god. we consumed all overflow entries, which means we have too many"
      << " hash collisions. page_id=" << assorted::Hex(page_id)
      << ", content=" << assorted::Hex(content)
      << ", ideal_bucket=" << assorted::Hex(ideal_bucket)
      << ", tag=" << assorted::Hex(tag)
      << ", page_id=" << assorted::Hex(page_id);
    return kErrorCodeCacheTooManyOverflow;
  }
  ASSERT_ND(new_overflow_entry < overflow_buckets_count_);
  overflow_free_buckets_head_ = overflow_buckets_[new_overflow_entry].next_;
  overflow_buckets_[new_overflow_entry].next_ = overflow_buckets_head_;
  overflow_buckets_[new_overflow_entry].refcount_.count_ = 1;
  overflow_buckets_[new_overflow_entry].bucket_ = new_bucket;
  assorted::memory_fence_release();
  overflow_buckets_head_ = new_overflow_entry;
  return kErrorCodeOk;
}

void CacheHashtable::evict(CacheHashtable::EvictArgs* args) {
  LOG(INFO) << "Snapshot-Cache eviction starts at node-" << numa_node_
    << ", clockhand_=" << clockhand_ << ", #target=" << args->target_count_;
  const BucketId end = get_physical_buckets();
  BucketId cur = clockhand_;

  // we check each entry in refcounts_, which are 2 bytes each.
  // for quicker checks, we want 8-byte aligned access.
  // also, we should anyway do prefetch, so make it 64-byte aligned
  cur = (cur >> 5) << 5;
  ASSERT_ND(cur % (1U << 5) == 0);
  if (cur >= end) {
    cur = 0;
  }

  // evict on the normal buckets first.
  args->evicted_count_ = 0;
  uint16_t loops;
  const uint16_t kMaxLoops = 16;  // if we need more loops than this, something is wrong...
  for (loops = 0; loops < kMaxLoops; ++loops) {
    cur = evict_main_loop(args, cur, loops);
    if (cur >= get_physical_buckets()) {
      cur = 0;
      // we went over all buckets in usual entries. now check the overflow linked list.
      if (overflow_buckets_head_) {
        evict_overflow_loop(args, loops);
      }
    }
    if (args->evicted_count_ >= args->target_count_) {
      break;
    } else {
      ASSERT_ND(cur == 0);  // we checked all buckets and wrapped around, right? go on to next loop
    }
  }

  clockhand_ = cur;
  LOG(INFO) << "Snapshot-Cache eviction completed at node-" << numa_node_
    << ", clockhand_=" << clockhand_ << ", #evicted=" << args->evicted_count_
    << ", looped-over the whole hashtable for " << loops << " times";
}

BucketId CacheHashtable::evict_main_loop(
  CacheHashtable::EvictArgs* args,
  BucketId cur,
  uint16_t loop) {
  ASSERT_ND(cur % (1U << 5) == 0);
  ASSERT_ND((assorted::kCachelineSize >> 5) == sizeof(CacheRefCount));
  const uint16_t decrements = 1U << loop;
  debugging::StopWatch watch;

  // the main idea is as follows.
  // whenever the bucket has never been used or been used but released without unlucky races,
  // the corresponding refcount is zero. hence, we just check for non-zeros in refcounts_.
  // this is trivially vectorized and the only observable cost is L1 cache miss.
  // we reduce L1 cache miss cost by prefetching a lot.
  uint32_t cur_cacheline = cur >> 5;
  const uint32_t end_cacheline = (get_physical_buckets() >> 5) + 1ULL;
  // for example, we prefetch cacheline 16-23 while reading cacheline 0-7.
  const uint16_t kL1PrefetchBatch = 8;
  const uint16_t kL1PrefetchAhead = 16;
  for (; cur_cacheline < end_cacheline; ++cur_cacheline) {
    if (cur_cacheline / kL1PrefetchBatch == 0) {
      assorted::prefetch_cachelines(
        refcounts_ + ((cur_cacheline + kL1PrefetchAhead) << 5),
        kL1PrefetchBatch);
    }

    BucketId bucket = cur_cacheline << 5;
    // gcc, you should be smart enough to optimize this. at least with O3.
    uint64_t* ints = reinterpret_cast<uint64_t*>(ASSUME_ALIGNED(refcounts_ + bucket, 64));
    bool all_zeros = true;
    for (uint16_t i = 0; i < 8U; ++i) {
      if (ints[i] != 0) {
        all_zeros = false;
        break;
      }
    }

    if (LIKELY(all_zeros)) {
      continue;
    } else {
      // this should be a rare case as far as we keep the hashtable sparse.
      CacheRefCount* base = reinterpret_cast<CacheRefCount*>(refcounts_ + bucket);
      for (uint16_t i = 0; i < 32U; ++i) {
        if (base[i].count_ > 0) {
          bool still_non_zero = base[i].decrement(decrements);
          if (!still_non_zero) {
            args->add_evicted(buckets_[bucket + i].get_content_id());
            buckets_[bucket + i].data_ = 0;
          }
        }
      }
    }

    if (args->evicted_count_ >= args->target_count_) {
      break;
    }
  }

  watch.stop();
  LOG(INFO) << "Snapshot-Cache eviction main_loop at node-" << numa_node_ << ", checked "
    << ((cur_cacheline << 5) - clockhand_) << " buckets in " << watch.elapsed_us() << "us";

  return cur_cacheline << 5;
}

void CacheHashtable::evict_overflow_loop(CacheHashtable::EvictArgs* args, uint16_t loop) {
  const uint16_t decrements = 1U << loop;
  uint32_t checked_count = 0;

  // store evicted entries into
  OverflowPointer evicted_head = 0;  // evicted
  debugging::StopWatch watch;
  {
    // We block this method entirely with the free buckets mutex.
    // This does NOT block usual transactions unless they actually have to newly add to overflow,
    // which should be very rare. This is cheap yet enough to make the free-list safe.
    soc::SharedMutexScope scope(&overflow_free_buckets_mutex_);

    // no interesting optimization. overflow list should be empty or almost empty.
    OverflowPointer head = overflow_buckets_head_;
    if (head != 0) {
      // skip the head. we handle it at the last.
      OverflowPointer prev = head;
      for (OverflowPointer cur = overflow_buckets_[prev].next_; cur != 0;) {
        CacheOverflowEntry* cur_entry = overflow_buckets_ + cur;
        OverflowPointer next = cur_entry->next_;
        bool still_non_zero = cur_entry->refcount_.decrement(decrements);
        if (!still_non_zero) {
          args->add_evicted(cur_entry->bucket_.get_content_id());
          CacheOverflowEntry* prev_entry = overflow_buckets_ + prev;
          prev_entry->next_ = next;
          cur_entry->bucket_.data_ = 0;
          cur_entry->next_ = evicted_head;
          evicted_head = cur;
        }

        prev = cur;
        cur = next;
        ++checked_count;
      }

      // finally check the head
      CacheOverflowEntry* cur_entry = overflow_buckets_ + head;
      bool still_non_zero = cur_entry->refcount_.decrement(decrements);
      if (!still_non_zero) {
        args->add_evicted(cur_entry->bucket_.get_content_id());
        overflow_buckets_head_ = cur_entry->next_;
        cur_entry->bucket_.data_ = 0;
        cur_entry->next_ = evicted_head;
        evicted_head = head;
      }
      ++checked_count;
    }
  }
  watch.stop();
  LOG(INFO) << "Snapshot-Cache eviction overflow_loop at node-" << numa_node_ << ", checked "
    << (checked_count) << " buckets in " << watch.elapsed_us() << "us";
}


std::ostream& operator<<(std::ostream& o, const HashFunc& v) {
  o << "<HashFunc>"
    << "<logical_buckets_>" << v.logical_buckets_ << "<logical_buckets_>"
    << "<physical_buckets_>" << v.physical_buckets_ << "<physical_buckets_>"
    << "</HashFunc>";
  return o;
}

ErrorStack CacheHashtable::verify_single_thread() const {
  for (BucketId i = 0; i < get_physical_buckets(); ++i) {
    if (buckets_[i].is_content_set()) {
      ASSERT_ND(buckets_[i].get_tag() != 0);
    } else {
      ASSERT_ND(buckets_[i].get_tag() == 0);
    }
  }
  return kRetOk;
}

CacheHashtable::Stat CacheHashtable::get_stat_single_thread() const {
  Stat result;
  result.normal_entries_ = 0;
  result.overflow_entries_ = 0;

  BucketId end = get_physical_buckets();
  for (BucketId i = 0; i < end; ++i) {
    if (buckets_[i].is_content_set()) {
      ++result.normal_entries_;
    }
  }

  if (overflow_buckets_head_) {
    for (OverflowPointer i = overflow_buckets_head_; i != 0;) {
      if (overflow_buckets_[i].bucket_.is_content_set()) {
        ++result.overflow_entries_;
      }
      i = overflow_buckets_[i].next_;
    }
  }

  return result;
}


ErrorCode CacheHashtable::find_batch(
  uint16_t batch_size,
  const storage::SnapshotPagePointer* page_ids,
  ContentId* out) const {
  if (batch_size == 0) {
    return kErrorCodeOk;
  }
  ASSERT_ND(batch_size <= kMaxFindBatchSize);
  if (UNLIKELY(batch_size > kMaxFindBatchSize)) {
    return kErrorCodeInvalidParameter;
  }

  // first, calculate hash values and prefetch
  BucketId bucket_numbers[kMaxFindBatchSize];
  for (uint16_t b = 0; b < batch_size; ++b) {
    if (page_ids[b] == 0) {
      continue;
    } else if (b > 0 && page_ids[b - 1] == page_ids[b]) {
      continue;
    }

    bucket_numbers[b] = get_bucket_number(page_ids[b]);
    ASSERT_ND(bucket_numbers[b] < get_logical_buckets());
    // we prefetch up to 128 bytes (16 entries).
    assorted::prefetch_cachelines(buckets_ + bucket_numbers[b], 2);
  }

  for (uint16_t b = 0; b < batch_size; ++b) {
    out[b] = 0;
    if (page_ids[b] == 0) {
      continue;
    } else if (b > 0 && page_ids[b - 1] == page_ids[b]) {
      out[b] = out[b - 1];
      continue;
    }

    PageIdTag tag = HashFunc::get_tag(page_ids[b]);
    ASSERT_ND(tag != 0);
    BucketId bucket_number = bucket_numbers[b];
    for (uint16_t i = 0; i < kHopNeighbors; ++i) {
      const CacheBucket& bucket = buckets_[bucket_number + i];
      if (bucket.get_tag() == tag) {
        // found (probably)!
        refcounts_[bucket_number + i].increment();
        out[b] = bucket.get_content_id();
        break;
      }
    }

    // Not found. let's check overflow list
    if (out[b] == 0 && overflow_buckets_head_) {
      for (OverflowPointer i = overflow_buckets_head_; i != 0;) {
        if (overflow_buckets_[i].bucket_.get_tag() == tag) {
          overflow_buckets_[i].refcount_.increment();
          out[b] = overflow_buckets_[i].bucket_.get_content_id();
          break;
        }
        i = overflow_buckets_[i].next_;
      }
    }
  }

  return kErrorCodeOk;
}

}  // namespace cache
}  // namespace foedus
