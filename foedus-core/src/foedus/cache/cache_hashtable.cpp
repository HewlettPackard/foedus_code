/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */

#include "foedus/cache/cache_hashtable.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace cache {

BucketId determine_logical_buckets(BucketId physical_buckets) {
  ASSERT_ND(physical_buckets >= 1024U);
  // to speed up, leave space in neighbors of the last bucket.
  // Instead, we do not wrap-around.
  BucketId buckets = physical_buckets - kHopNeighbors;

  // to make the division-hashing more effective, make it prime-like.
  BucketId logical_buckets = assorted::generate_almost_prime_below(buckets);
  ASSERT_ND(logical_buckets <= physical_buckets);
  ASSERT_ND((logical_buckets & (logical_buckets - 1U)) != 0);  // at least not power of 2
  return logical_buckets;
}

uint32_t determine_overflow_list_size(BucketId physical_buckets) {
  // there should be very few overflow entries. This can be super small.
  const uint32_t kOverflowFraction = 1024U;
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
  hash_func_(physical_buckets) {
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
  buckets_[0].set_content_id(0);
  buckets_[0].set_tag(0);
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
  overflow_buckets_[0].bucket_.set_content_id(0);
  overflow_buckets_[0].bucket_.set_tag(0);
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
  new_bucket.set_content_id(content);
  new_bucket.set_tag(tag);

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


}  // namespace cache
}  // namespace foedus
