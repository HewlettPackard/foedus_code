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

HashFunc::HashFunc(BucketId physical_buckets)
  : logical_buckets_(determine_logical_buckets(physical_buckets)),
    physical_buckets_(physical_buckets),
    bucket_div_(logical_buckets_) {
}

CacheHashtable::CacheHashtable(BucketId physical_buckets, uint16_t numa_node)
  : numa_node_(numa_node),
  hash_func_(physical_buckets) {
  buckets_memory_.alloc(
    sizeof(CacheBucket) * physical_buckets,
    1U << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node);
  buckets_ = reinterpret_cast<CacheBucket*>(buckets_memory_.get_block());
}

bool CacheBucket::atomic_status_cas(CacheBucketStatus expected, CacheBucketStatus desired) {
  return assorted::raw_atomic_compare_exchange_strong<uint64_t>(
    &status_.data_,
    &expected.data_,
    desired.data_);
}

void CacheBucket::atomic_set_hop_bit(uint16_t hop) {
  while (true) {
    CacheBucketStatus cur_status = status_;
    ASSERT_ND(!cur_status.is_hop_bit_on(hop));
    CacheBucketStatus new_status = cur_status;
    new_status.set_hop_bit_on(hop);
    if (atomic_status_cas(cur_status, new_status)) {
      break;
    }
  }
}
void CacheBucket::atomic_unset_hop_bit(uint16_t hop) {
  while (true) {
    CacheBucketStatus cur_status = status_;
    ASSERT_ND(cur_status.is_hop_bit_on(hop));
    CacheBucketStatus new_status = cur_status;
    new_status.set_hop_bit_off(hop);
    if (atomic_status_cas(cur_status, new_status)) {
      break;
    }
  }
}

void CacheBucket::atomic_empty() {
  page_id_ = 0;
  while (true) {
    CacheBucketStatus cur_status = status_;
    CacheBucketStatus new_status = cur_status;
    new_status.set_content_id(0);
    new_status.set_hop_bit_off(0);
    if (atomic_status_cas(cur_status, new_status)) {
      break;
    }
  }
}

bool CacheBucket::try_occupy_unused_bucket() {
  CacheBucketStatus cur_status = status_;
  if (cur_status.is_content_set() || cur_status.is_being_modified()) {
    return false;
  }
  CacheBucketStatus new_status = cur_status;
  new_status.set_being_modified();
  ASSERT_ND(!cur_status.is_being_modified());
  ASSERT_ND(new_status.is_being_modified());
  bool success = atomic_status_cas(cur_status, new_status);
  if (!success) {
    DVLOG(0) << "Interesting. lost race for occupying bucket";
  } else {
    ASSERT_ND(status_.is_being_modified());
    ASSERT_ND(!status_.is_content_set());
  }
  return success;
}

BucketId CacheHashtable::find_next_empty_bucket(BucketId from_bucket) const {
  const BucketId physical_buckets = get_physical_buckets();
  for (BucketId bucket = from_bucket + 1; bucket < physical_buckets; ++bucket) {
    if (buckets_[bucket].page_id_ == 0
        && !buckets_[bucket].status_.is_content_set()
        && !buckets_[bucket].status_.is_being_modified()) {
      return bucket;
    }
  }
  return kBucketNotFound;
}

ErrorCode CacheHashtable::grab_unused_bucket(
  storage::SnapshotPagePointer page_id,
  BucketId from_bucket,
  BucketId* occupied_bucket) {
  *occupied_bucket = kBucketNotFound;
  {
    CacheBucket& bucket = buckets_[from_bucket];
    if (bucket.try_occupy_unused_bucket()) {
      *occupied_bucket = from_bucket;
      DVLOG(2) << "Okay, grabbed best position for page_id " << assorted::Hex(page_id)
        << ". bucket-" << from_bucket;
      return kErrorCodeOk;
    }
  }

  const BucketId physical_buckets = get_physical_buckets();
  BucketId empty_bucket = from_bucket;
  while (true) {
    empty_bucket = find_next_empty_bucket(empty_bucket);
    if (empty_bucket == kBucketNotFound) {
      LOG(ERROR) << "Could not find an empty bucket while cache miss. "
        << ", bucket=" << from_bucket << ", cur=" << empty_bucket;
      return kErrorCodeCacheTableFull;
    }
    ASSERT_ND(empty_bucket > 0 && empty_bucket < physical_buckets);
    CacheBucketStatus empty_status = buckets_[empty_bucket].status_;
    while (empty_bucket - from_bucket >= kHopNeighbors) {
      DVLOG(0) << "Mmm, it's a bit too far(cur=" << empty_bucket << ", bucket=" << from_bucket
        << "). we must move the hole towards it."
        << " For the best performance, we had to make sure this won't happen...";
      LOG(FATAL) << "TODO. this part must be debugged";
      BucketId back;
      for (back = 1; back < kHopNeighbors; ++back) {
        storage::SnapshotPagePointer target_page_id = buckets_[empty_bucket - back].page_id_;
        BucketId original = get_bucket_number(target_page_id);
        BucketId original_hop = empty_bucket - back - original;
        if (original + kHopNeighbors <= empty_bucket + 1U) {
          // okay, then we can move this to empty_bucket.
          CacheBucketStatus new_status = empty_status;
          new_status.set_content_id(buckets_[empty_bucket - back].status_.get_content_id());
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
          << ", bucket=" << from_bucket << ", cur=" << empty_bucket;
        return kErrorCodeCacheTableFull;
      } else {
        empty_bucket -= back;
      }
    }

    // Now it should be a neighbor. just insert to there
    BucketId hop = empty_bucket - from_bucket;
    ASSERT_ND(hop < kHopNeighbors);
    CacheBucket& neighbor = buckets_[from_bucket + hop];
    if (neighbor.try_occupy_unused_bucket()) {
      *occupied_bucket = from_bucket + hop;
      DVLOG(2) << "Okay, grabbed an alternative position for page_id" << assorted::Hex(page_id)
        << " bucket-" << empty_bucket << "(hop=" << hop << ")";
      return kErrorCodeOk;
    }
  }

  return kErrorCodeCacheTableFull;
}


ErrorCode CacheHashtable::install_missed_page(
  storage::SnapshotPagePointer page_id,
  ContentId* out,
  PageReadCallback cachemiss_callback,
  void* cachemiss_context) {
  *out = 0;

  // Grab a bucket to install a new page.
  // The bucket does not have to be the only bucket to serve the page, so
  // the logic below is much simpler than typical bufferpool.
  BucketId ideal_bucket = get_bucket_number(page_id);
  BucketId occupied_bucket = kBucketNotFound;
  CHECK_ERROR_CODE(grab_unused_bucket(page_id, ideal_bucket, &occupied_bucket));
  ASSERT_ND(occupied_bucket != kBucketNotFound);
  BucketId hop = occupied_bucket - ideal_bucket;
  ASSERT_ND(hop < kHopNeighbors);

  CacheBucket& bucket = buckets_[occupied_bucket];
  ContentId callback_out;
  ErrorCode callback_result = cachemiss_callback(this, cachemiss_context, page_id, &callback_out);
  if (callback_result != kErrorCodeOk) {
    LOG(ERROR) << "Umm cachemiss callback returned an error. PageId= " << assorted::Hex(page_id)
      << " Releasing the bucket-" << occupied_bucket << "...";
    bucket.status_.unset_being_modified();
    return callback_result;
  }

  CacheBucketStatus new_status = bucket.status_;  // 8 bytes regular read
  ASSERT_ND(new_status.is_being_modified());
  bucket.page_id_ = page_id;
  new_status.set_hop_bit_on(hop);
  new_status.unset_being_modified();
  new_status.set_content_id(callback_out);
  assorted::memory_fence_release();
  bucket.status_ = new_status;  // also unlock. 8 bytes atomic write.
  *out = callback_out;
  return kErrorCodeOk;
}

std::ostream& operator<<(std::ostream& o, const HashFunc& v) {
  o << "<HashFunc>"
    << "<logical_buckets_>" << v.logical_buckets_ << "<logical_buckets_>"
    << "<physical_buckets_>" << v.physical_buckets_ << "<physical_buckets_>"
    << "</HashFunc>";
  return o;
}

}  // namespace cache
}  // namespace foedus
