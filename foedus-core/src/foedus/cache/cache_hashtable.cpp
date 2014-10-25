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

uint32_t determine_logical_buckets(uint32_t physical_buckets) {
  ASSERT_ND(physical_buckets >= 1024U);
  // to speed up, leave space in neighbors of the last bucket.
  // Instead, we do not wrap-around.
  uint32_t buckets = physical_buckets - CacheHashtable::kHopNeighbors;

  // to make the division-hashing more effective, make it prime-like.
  uint32_t logical_buckets = assorted::generate_almost_prime_below(buckets);
  ASSERT_ND(logical_buckets <= physical_buckets);
  ASSERT_ND((logical_buckets & (logical_buckets - 1U)) != 0);  // at least not power of 2
  return logical_buckets;
}

HashFunc::HashFunc(uint32_t physical_buckets)
  : logical_buckets_(determine_logical_buckets(physical_buckets)),
    physical_buckets_(physical_buckets),
    bucket_div_(logical_buckets_) {
}

CacheHashtable::CacheHashtable(
  const memory::AlignedMemory& table_memory,
  storage::Page* cache_base)
  : hash_func_(table_memory.get_size() / sizeof(CacheHashtable::Bucket)),
  buckets_(reinterpret_cast<Bucket*>(table_memory.get_block())),
  cache_base_(cache_base) {
  LOG(INFO) << "Initialized CacheHashtable. node=" << table_memory.get_numa_node()
    << ", hash_func=" << hash_func_;
}

uint32_t CacheHashtable::find_next_empty_bucket(uint32_t from_bucket) const {
  uint32_t physical_buckets = get_physical_buckets();
  for (uint32_t bucket = from_bucket + 1; bucket < physical_buckets; ++bucket) {
    if (buckets_[bucket].status_.components.offset == 0) {
      return bucket;
    }
  }
  return 0xFFFFFFFFU;
}

ErrorCode CacheHashtable::miss(
  storage::SnapshotPagePointer page_id,
  thread::Thread* context,
  storage::Page** out) {
  *out = nullptr;

  // grab a buffer page and read into it.
  memory::PagePoolOffset offset = context->get_thread_memory()->grab_free_snapshot_page();
  if (offset == 0) {
    // TODO(Hideaki) First, we have to make sure this doesn't happen often (cleaner's work).
    // Second, when this happens, we have to do eviction now, but probably after aborting the xct.
    LOG(ERROR) << "Could not grab free snapshot page while cache miss. thread=" << *context
      << ", page_id=" << assorted::Hex(page_id) << " this=" << *this;
    return kErrorCodeCacheNoFreePages;
  }
  storage::Page* new_page = cache_base_ + offset;
  CHECK_ERROR_CODE(context->read_a_snapshot_page(page_id, new_page));
  *out = new_page;

  // Successfully read it. Now, for the following accesses, let's install it to the hash table
  uint32_t bucket_number = get_bucket_number(page_id);
  Bucket& bucket = buckets_[bucket_number];
  {
    BucketStatus cur_status = bucket.status_;
    if (cur_status.components.offset == 0) {
      BucketStatus new_status = cur_status;
      new_status.components.hop_bitmap |= 0x1;
      new_status.components.offset = offset;
      if (bucket.atomic_status_cas(cur_status, new_status)) {
        // successfully installed the new page in this empty bucket. should be mostly this case.
        DVLOG(2) << "Okay, inserted page_id " << assorted::Hex(page_id)
          << " to the best position; bucket-" << bucket_number;
        bucket.page_id_ = page_id;
        return kErrorCodeOk;
      } else {
        VLOG(0) << "Interesting. lost race for page_id " << assorted::Hex(page_id)
          << " to the best position; bucket-" << bucket_number;
        // that means someone has just modified this bucket... maybe the exact page we want?
        assorted::memory_fence_acquire();  // atomic ops above implies it, but to clarify.
        if (bucket.status_.components.offset != 0 && bucket.page_id_ == page_id) {
          VLOG(0) << "Lucky, someone else has just inserted the wanted page!";
          *out = cache_base_ + bucket.status_.components.offset;
          ASSERT_ND((*out)->get_header().page_id_ == page_id);
          context->get_thread_memory()->release_free_snapshot_page(offset);
          return kErrorCodeOk;
        }
      }
    }
  }

  const uint32_t physical_buckets = get_physical_buckets();
  uint32_t empty_bucket = bucket_number;
  while (true) {
    empty_bucket = find_next_empty_bucket(empty_bucket);
    if (empty_bucket == 0xFFFFFFFFU) {
      LOG(ERROR) << "Could not find an empty bucket while cache miss. "
        << ", bucket=" << bucket_number << ", cur=" << empty_bucket << " this=" << *this;
      context->get_thread_memory()->release_free_snapshot_page(offset);
      return kErrorCodeCacheTableFull;
    }
    ASSERT_ND(empty_bucket > 0 && empty_bucket < physical_buckets);
    BucketStatus empty_status = buckets_[empty_bucket].status_;
    if (empty_status.components.offset > 0) {
      continue;
    }

    while (empty_bucket - bucket_number >= kHopNeighbors) {
      DVLOG(0) << "Mmm, it's a bit too far(cur=" << empty_bucket << ", bucket=" << bucket_number
        << "). we must move the hole towards it."
        << " For the best performance, we had to make sure this won't happen..." << *this;
      uint32_t back;
      for (back = 1; back < kHopNeighbors; ++back) {
        storage::SnapshotPagePointer target_page_id = buckets_[empty_bucket - back].page_id_;
        uint32_t original = get_bucket_number(target_page_id);
        uint32_t original_hop = empty_bucket - back - original;
        if (original <= empty_bucket + 1 - kHopNeighbors) {
          // okay, then we can move this to empty_bucket.
          BucketStatus new_status = empty_status;
          new_status.components.offset = buckets_[empty_bucket - back].status_.components.offset;
          if (buckets_[empty_bucket].atomic_status_cas(empty_status, new_status)) {
            ASSERT_ND(buckets_[original].is_hop_bit_on(original_hop));
            buckets_[empty_bucket].page_id_ = target_page_id;
            // TODO(Hideaki) the following should be one function for better performance. minor.
            buckets_[original].atomic_unset_hop_bit(original_hop);
            buckets_[original].atomic_set_hop_bit(empty_bucket - original);

            buckets_[empty_bucket - back].atomic_empty();
            DVLOG(0) << "Okay, moved a hole from " << empty_bucket
              << " to " << (empty_bucket - back) << "(original=" << original << ")";
            break;
          } else {
            VLOG(0) << "Lost race while moving a hole from " << empty_bucket
              << " to " << (empty_bucket - back) << "(original=" << original << ")";
          }
        }
      }

      // if we reach here, we must resize the table. we so far refuse the insert in the case.
      // we don't fill up the hashtable more than 50%, so this shouldn't happen.
      if (back == kHopNeighbors) {
        LOG(ERROR) << "Could not find an empty bucket while moving holes towards bucket. "
          << ", bucket=" << bucket_number << ", cur=" << empty_bucket << " this=" << *this;
        context->get_thread_memory()->release_free_snapshot_page(offset);
        return kErrorCodeCacheTableFull;
      } else {
        empty_bucket -= back;
      }
    }

    // Now it should be a neighbor. just insert to there
    uint32_t hop = empty_bucket - bucket_number;
    ASSERT_ND(hop < kHopNeighbors);
    Bucket& neighbor = buckets_[bucket_number + hop];
    BucketStatus neighbor_status = neighbor.status_;
    if (neighbor_status.components.offset == 0) {
      BucketStatus neighbor_new_status = neighbor_status;
      neighbor_new_status.components.offset = offset;
      if (neighbor.atomic_status_cas(neighbor_status, neighbor_new_status)) {
        neighbor.atomic_set_hop_bit(hop);
        neighbor.page_id_ = page_id;
        DVLOG(2) << "Okay, inserted page_id" << assorted::Hex(page_id)
          << " to an alternative position; bucket-" << empty_bucket << "(hop=" << hop << ")";
        return kErrorCodeOk;
      } else {
        VLOG(0) << "Interesting. lost race for page_id " << assorted::Hex(page_id)
          << " to an alternative position; bucket-" << empty_bucket << "(hop=" << hop << ")";
        assorted::memory_fence_acquire();
        if (neighbor.status_.components.offset != 0 && neighbor.page_id_ == page_id) {
          VLOG(0) << "Lucky, someone else has just inserted the wanted page!";
          *out = cache_base_ + neighbor.status_.components.offset;
          ASSERT_ND((*out)->get_header().page_id_ == page_id);
          context->get_thread_memory()->release_free_snapshot_page(offset);
          return kErrorCodeOk;
        }
      }
    }
  }


  return kErrorCodeOk;
}

bool CacheHashtable::Bucket::atomic_status_cas(BucketStatus expected, BucketStatus desired) {
  return assorted::raw_atomic_compare_exchange_strong<uint64_t>(
    &status_.word,
    &expected.word,
    desired.word);
}

void CacheHashtable::Bucket::atomic_set_hop_bit(uint16_t hop) {
  while (true) {
    BucketStatus cur_status = status_;
    ASSERT_ND((cur_status.components.hop_bitmap & (1U << hop)) == 0U);
    BucketStatus new_status = cur_status;
    new_status.components.hop_bitmap |= (1U << hop);
    if (atomic_status_cas(cur_status, new_status)) {
      break;
    }
  }
}
void CacheHashtable::Bucket::atomic_unset_hop_bit(uint16_t hop) {
  while (true) {
    BucketStatus cur_status = status_;
    ASSERT_ND((cur_status.components.hop_bitmap & (1U << hop)) != 0U);
    BucketStatus new_status = cur_status;
    new_status.components.hop_bitmap ^= (1U << hop);
    if (atomic_status_cas(cur_status, new_status)) {
      break;
    }
  }
}

void CacheHashtable::Bucket::atomic_empty() {
  page_id_ = 0;
  while (true) {
    BucketStatus cur_status = status_;
    BucketStatus new_status = cur_status;
    new_status.components.offset = 0;
    if ((new_status.components.hop_bitmap & 1U) != 0U) {
      new_status.components.hop_bitmap ^= 1U;
    }
    if (atomic_status_cas(cur_status, new_status)) {
      break;
    }
  }
}

std::ostream& operator<<(std::ostream& o, const HashFunc& v) {
  o << "<HashFunc>"
    << "<logical_buckets_>" << v.logical_buckets_ << "<logical_buckets_>"
    << "<physical_buckets_>" << v.physical_buckets_ << "<physical_buckets_>"
    << "</HashFunc>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const CacheHashtable& v) {
  o << "<CacheHashtable>"
    << v.hash_func_
    << "<cache_base_>" << v.cache_base_ << "<cache_base_>"
    << "</CacheHashtable>";
  return o;
}

}  // namespace cache
}  // namespace foedus
