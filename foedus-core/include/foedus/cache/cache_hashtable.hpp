/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_HASHTABLE_HPP_
#define FOEDUS_CACHE_CACHE_HASHTABLE_HPP_

#include <stdint.h>
#include <xmmintrin.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace cache {
/**
 * @brief A NUMA-local hashtable of cached snapshot pages.
 * @ingroup CACHE
 * @details
 * @section CACHE_HOPSCOTCH Hopscotch Hashtable
 * We use Herlihy's Hopscotch hashtable without heavy concurrency control.
 * Thanks to the loose requirements, we don't need most of concurrency control the original
 * paper does.
 *
 * @section CACHE_BUCKET Hash bucket in cache table
 * Each bucket is 16 bytes, consisting of full page ID, hopscotch bitmap, and offset of pointed
 * cache page. As each page is 4kb and we expect to spend GBs of DRAM for snapshot cache,
 * the table will have many millions of buckets. However, we can probably assume that
 * the size is within 32 bit. Even with 50% fullness, 2^32 / 2 * 4kb = 8TB.
 * We will not have that much DRAM per NUMA node.
 *
 * @section CACHE_HASH Hash Function in cache table
 * We have a fixed type of key, SnapshotPagePointer. For the fastest calculation of
 * the hash we simply divide by the size of hashtable, which we adjust to be \e almost a prime
 * number (in many cases actually a prime). We use assorted::ConstDiv to speed up the division.
 */
class CacheHashtable CXX11_FINAL {
 public:
  enum Costants {
    kHopNeighbors = 32,
  };
  union BucketStatus {
    uint64_t                  word;
    struct Components {
      /**
      * The hop bitmap in Hopscotch hashing algorithm.
      * n-th bit means whether n-th bucket from this bucket contains an entry whose hash belongs to
      * this bucket.
      */
      uint32_t                hop_bitmap;

      /** Offset in cache buffer of the entry. 0 if empty bucket. */
      memory::PagePoolOffset  offset;
    } components;
  };

  /** A bucket in the Hopscotch hashtable. */
  struct Bucket {
    /** The full key of the entry. 0 if empty bucket. */
    storage::SnapshotPagePointer  page_id_;     // +8 -> 8
    BucketStatus                  status_;      // +8 -> 16

    bool is_hop_bit_on(uint16_t hop) const {
      return (status_.components.hop_bitmap & (1U << hop)) != 0;
    }
    bool atomic_status_cas(BucketStatus expected, BucketStatus desired);
    void atomic_unset_hop_bit(uint16_t hop);
    void atomic_set_hop_bit(uint16_t hop);
    void atomic_empty();
  };

  CacheHashtable(const memory::AlignedMemory& table_memory, storage::Page* cache_base);

  ErrorCode read_page(
    storage::SnapshotPagePointer page_id,
    thread::Thread* context,
    storage::Page** out) ALWAYS_INLINE;

  friend std::ostream& operator<<(std::ostream& o, const CacheHashtable& v);

 private:
  /** Number of buckets in this table. */
  const uint32_t            table_size_;
  /** table_size_ is at least kHopNeighbors smaller than this. */
  const uint32_t            physical_table_size_;
  /** to efficiently divide a number by bucket_div_. */
  const assorted::ConstDiv  bucket_div_;
  Bucket* const             buckets_;
  storage::Page* const      cache_base_;

  /**
   * Returns a bucket number the given page ID \e should belong to.
   */
  uint32_t get_bucket_number(storage::SnapshotPagePointer page_id) const ALWAYS_INLINE;

  /**
   * @brief Returns an offset for the given page ID \e conservatively.
   * @param[in] page_id Page ID to look for
   * @return offset that contains the page. 0 if not found.
   * @details
   * This doesn't take a lock, so a concurrent thread might have inserted the wanted page
   * concurrrently. It's fine. Then we read the page from snapshot, just wasting a bit.
   * Instead, an entry is removed from the cache gracefully, thus an offset we observed
   * will not become invalid soon (pages are garbage collected with grace period).
   * No precise concurrency control needed.
   */
  uint32_t conservatively_locate(storage::SnapshotPagePointer page_id) const ALWAYS_INLINE;

  uint32_t find_next_empty_bucket(uint32_t from_bucket) const;

  /**
   * @brief Called when a cached page is not found.
   * @details
   * Reads the page from snapshot file and put it as a newly cached page.
   * This method is the core of our implementation of hopscotch algorithm.
   * We are anyway doing at least 4kb memory copy in this case, so no need for serious optimization.
   */
  ErrorCode miss(
    storage::SnapshotPagePointer page_id,
    thread::Thread* context,
    storage::Page** out);
};

inline uint32_t CacheHashtable::get_bucket_number(storage::SnapshotPagePointer page_id) const {
  uint64_t reversed = __builtin_bswap64(page_id);  // TODO(Hideaki) non-GCC
  uint64_t quotient = bucket_div_.div64(reversed);
  return bucket_div_.rem64(page_id, table_size_, quotient);
}

inline memory::PagePoolOffset CacheHashtable::conservatively_locate(
  storage::SnapshotPagePointer page_id) const {
  ASSERT_ND(page_id > 0);
  uint32_t bucket_number = get_bucket_number(page_id);
  ASSERT_ND(bucket_number < table_size_);
  const Bucket& bucket = buckets_[bucket_number];
  uint32_t hop_bitmap = bucket.status_.components.hop_bitmap;
  if (hop_bitmap == 0) {
    return 0;
  }
  // the ideal, and most possible case is that the exact bucket contains it.
  if (bucket.page_id_ == page_id) {
    // if the page ID exactly matches, we can assume the offset is valid for a while.
    // but, we have to make sure the cleaner thread is not removing this now.
    // for that, we have to take a fence and then re-check page ID.
    memory::PagePoolOffset offset = bucket.status_.components.offset;
    // not acquire. as far as we re-check page ID BEFORE offset, it's safe.
    assorted::memory_fence_consume();
    if (bucket.page_id_ == page_id) {
      return offset;
    }
  }
  // otherwise, we check neighbors. To speed up, we prefetch each candidate 64 bits.
  // Each bucket is 16 bytes, so for each 4 entries.
  for (uint8_t i = 1; i < (kHopNeighbors / 4); ++i) {
    if (((hop_bitmap >> (i * 4)) & 0xF) != 0) {
      assorted::prefetch_cacheline(buckets_ + bucket_number + i);
    }
  }
  // now, check each bucket up to kHopNeighbors. Again, this is a probabilistic search.
  // something might happen concurrently, but we don't care.
  for (uint8_t i = 1; i < kHopNeighbors; ++i) {
    if (bucket.status_.components.hop_bitmap & (1 << i)) {
      const Bucket& another_bucket = buckets_[bucket_number + i];
      if (another_bucket.page_id_ == page_id) {
        memory::PagePoolOffset offset = another_bucket.status_.components.offset;
        assorted::memory_fence_consume();
        if (another_bucket.page_id_ == page_id) {
          return offset;
        }
      }
    }
  }
  return 0;
}

inline ErrorCode CacheHashtable::read_page(
  storage::SnapshotPagePointer page_id,
  thread::Thread* context,
  storage::Page** out) {
  memory::PagePoolOffset offset = conservatively_locate(page_id);
  if (offset != 0) {
    *out = cache_base_ + offset;
    // conservatively_locate() locates the page conservatively, so it might have false negatives
    // (missing concurrently inserted cache page). but it must not have false positives.
    ASSERT_ND((*out)->get_header().page_id_ == page_id);
    return kErrorCodeOk;
  } else {
    return miss(page_id, context, out);
  }
}

}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_HASHTABLE_HPP_
