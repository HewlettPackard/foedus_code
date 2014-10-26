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
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace cache {

// these two are actually of the same integer type, but have very different meanings.
/**
 * Offset in hashtable bucket.
 */
typedef uint32_t BucketId;
/**
 * Offset of the actual page image in PagePool for snapshot pages. 0 means not set yet.
 */
typedef memory::PagePoolOffset ContentId;

/** 32th bit is used for concurrency control, so up to 31. */
const uint32_t kHopNeighbors = 31;
const BucketId kBucketNotFound = 0xFFFFFFFFU;

/**
 * @brief A simple hash function logic used in snasphot cache cache.
 * @ingroup CACHE
 * The hash key is SnapshotPagePointer, whose least significant bits are local page IDs, which
 * are sequnetial. To avoid placing adjacent page IDs in adjacent hash buckets, we
 * use byte-swap and a few bit shifts, then divide by logical table size which is
 * adjusted to an almost-prime number.
 */
struct HashFunc CXX11_FINAL {
  /** Number of logical (actually used) buckets in this table. */
  const BucketId            logical_buckets_;
  /** Number of buckets that are physically allocated, at least kHopNeighbors more than logical. */
  const BucketId            physical_buckets_;
  /** to efficiently divide a number by bucket_div_. */
  const assorted::ConstDiv  bucket_div_;

  explicit HashFunc(BucketId physical_buckets);

  /**
   * Returns a hash value for the given snapshot page ID. This needs no member variables.
   */
  static uint32_t get_hash(storage::SnapshotPagePointer page_id) ALWAYS_INLINE;

  /**
   * Returns a bucket number the given snapshot page ID \e should belong to.
   */
  BucketId get_bucket_number(storage::SnapshotPagePointer page_id) const ALWAYS_INLINE {
    uint32_t seed = get_hash(page_id);
    uint32_t quotient = bucket_div_.div32(seed);
    uint32_t bucket = bucket_div_.rem32(seed, logical_buckets_, quotient);
    ASSERT_ND(bucket == seed % logical_buckets_);
    return bucket;
  }

  friend std::ostream& operator<<(std::ostream& o, const HashFunc& v);
};

const uint64_t kBucketBeingModified = 1ULL << 63;

/**
 * @brief Represents a status of CacheBucket.
 * @details
 * Highest bit is for concurrency control. If it's ON, other writers can't write.
 * Readers skip such buckets, and it's safe to do so thanks to the loose requirement in the
 * snapshot cache.
 */
struct CacheBucketStatus CXX11_FINAL {
  uint64_t data_;

  /**
   * The hop bitmap in Hopscotch hashing algorithm.
   * n-th bit means whether n-th bucket from this bucket contains an entry whose hash belongs to
   * this bucket.
   */
  uint32_t  get_hop_bitmap() const { return (data_ >> 32) & 0x7FFFFFFFU; }
  ContentId get_content_id() const { return data_; }
  void      set_content_id(ContentId id) { data_ = (data_ & 0xFFFFFFFF00000000ULL) | id; }
  bool      is_content_set() const { return get_content_id() != 0; }
  bool      is_hop_bit_on(uint16_t hop) const { return (data_ & (1ULL << (hop + 32))) != 0; }
  void      set_hop_bit_on(uint16_t hop) {
    ASSERT_ND(!is_hop_bit_on(hop));
    data_ |= (1ULL << (hop + 32));
  }
  void      set_hop_bit_off(uint16_t hop) {
    ASSERT_ND(is_hop_bit_on(hop));
    data_ &= ~(1ULL << (hop + 32));
  }
  bool      is_being_modified() const { return (data_ & kBucketBeingModified) != 0; }
  void      set_being_modified() {
    ASSERT_ND(!is_being_modified());
    data_ |= kBucketBeingModified;
  }
  void      unset_being_modified() {
    ASSERT_ND(is_being_modified());
    data_ &= ~kBucketBeingModified;
  }
};


/**
 * @brief Hash bucket in cache table.
 * @ingroup CACHE
 * @details
 * Each bucket is 16 bytes, consisting of full page ID, hopscotch bitmap, and offset of pointed
 * cache page. As each page is 4kb and we expect to spend GBs of DRAM for snapshot cache,
 * the table will have many millions of buckets. However, we can probably assume that
 * the size is within 32 bit. Even with 50% fullness, 2^32 / 2 * 4kb = 8TB.
 * We will not have that much DRAM per NUMA node.
 */
struct CacheBucket CXX11_FINAL {
  /** The full key of the entry. 0 if empty bucket. */
  storage::SnapshotPagePointer  page_id_;     // +8 -> 8
  CacheBucketStatus             status_;      // +8 -> 16

  bool is_hop_bit_on(uint16_t hop) const { return status_.is_hop_bit_on(hop); }
  bool is_content_set() const { return status_.is_content_set(); }
  bool atomic_status_cas(CacheBucketStatus expected, CacheBucketStatus desired);
  void atomic_unset_hop_bit(uint16_t hop);
  void atomic_set_hop_bit(uint16_t hop);
  void atomic_empty();
  bool try_occupy_unused_bucket();
};

/**
 * @brief A callback function invoked for each page cache miss in CacheHashtable.
 * @ingroup CACHE
 * We separate out the page-read logic from CacheHashtable for better testability.
 * The callback is implemented in thread_pimpl.
 */
typedef ErrorCode (*PageReadCallback)(
  CacheHashtable* table,  // for future use. we do not use this param so far.
  void* context,
  storage::SnapshotPagePointer page_id,
  ContentId* content_id);


/**
 * @brief A NUMA-local hashtable of cached snapshot pages.
 * @ingroup CACHE
 * @details
 * @section CACHE_HOPSCOTCH Hopscotch Hashtable
 * We use Herlihy's Hopscotch hashtable without heavy concurrency control.
 * Thanks to the loose requirements, we don't need most of concurrency control the original
 * paper does.
 *
 * @section CACHE_HASH Hash Function in cache table
 * We have a fixed type of key, SnapshotPagePointer. For the fastest calculation of
 * the hash we simply divide by the size of hashtable, which we adjust to be \e almost a prime
 * number (in many cases actually a prime). We use assorted::ConstDiv to speed up the division.
 */
class CacheHashtable CXX11_FINAL {
 public:
  CacheHashtable(BucketId physical_buckets, uint16_t numa_node);

  /**
   * @brief Main API of this object.
   * @details
   * In fact, this is the only public method used in normal execution.
   * Other public methods below are for testcases (they are const, wouldn't hurt to make public).
   */
  ErrorCode retrieve(
    storage::SnapshotPagePointer page_id,
    ContentId* out,
    PageReadCallback cachemiss_callback,
    void* cachemiss_context) ALWAYS_INLINE;

  BucketId get_logical_buckets() const ALWAYS_INLINE { return hash_func_.logical_buckets_; }
  BucketId get_physical_buckets() const ALWAYS_INLINE { return hash_func_.physical_buckets_; }

  /**
   * Returns a bucket number the given page ID \e should belong to.
   */
  BucketId get_bucket_number(storage::SnapshotPagePointer page_id) const ALWAYS_INLINE {
    return hash_func_.get_bucket_number(page_id);
  }

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
  BucketId conservatively_locate(storage::SnapshotPagePointer page_id) const ALWAYS_INLINE;

  BucketId find_next_empty_bucket(BucketId from_bucket) const;


 protected:
  const uint16_t            numa_node_;
  const HashFunc            hash_func_;
  memory::AlignedMemory     buckets_memory_;
  CacheBucket*              buckets_;

  /**
   * @brief Called after a cached page is not found and installed.
   * @details
   * This method is the core of our implementation of hopscotch algorithm.
   * We are anyway doing at least 4kb memory copy in this case, so no need for serious optimization.
   */
  ErrorCode install_missed_page(
    storage::SnapshotPagePointer page_id,
    ContentId* out,
    PageReadCallback cachemiss_callback,
    void* cachemiss_context);

  ErrorCode grab_unused_bucket(
    storage::SnapshotPagePointer page_id,
    BucketId from_bucket,
    BucketId* occupied_bucket);
  // BucketId grab_unused_bucket(BucketId from_bucket);
};

inline uint32_t HashFunc::get_hash(storage::SnapshotPagePointer page_id) {
  uint16_t snapshot_id = storage::extract_snapshot_id_from_snapshot_pointer(page_id);
  uint8_t numa_node = storage::extract_numa_node_from_snapshot_pointer(page_id);
  uint64_t local_page_id = storage::extract_local_page_id_from_snapshot_pointer(page_id);
  // snapshot_id is usually very sparse. and has no correlation with local_page_id.
  // numa_node too. thus just use them as seeds for good old multiplicative hashing.
  uint32_t seed = snapshot_id * 0x5a2948074497175aULL;
  seed += numa_node * 0xc30a95f6e63dd908ULL;

  // local_page_id is 40 bits. the highest 8 bits are very sparse, and it has no correlation with
  // the first 8 bits (how come 4kb-th page and 16Tb+4kb-th page has anything in common..).
  // so, let's combine highest 8 bits and lowest 8 bits.
  uint32_t folded_local_page_id = local_page_id ^ static_cast<uint32_t>(local_page_id >> 32);

  // now, we don't want to place adjacent pages in adjacent hash buckets. so, let's swap bytes.
  // this benefits hop-scotch hashing table.
  uint32_t swaped_page_id = __builtin_bswap32(folded_local_page_id);
  seed ^= swaped_page_id;
  return seed;
}


inline ContentId CacheHashtable::conservatively_locate(
  storage::SnapshotPagePointer page_id) const {
  ASSERT_ND(page_id > 0);
  uint32_t bucket_number = get_bucket_number(page_id);
  ASSERT_ND(bucket_number < get_logical_buckets());
  const CacheBucket& bucket = buckets_[bucket_number];
  CacheBucketStatus status = bucket.status_;  // this is guaranteed to be a regular read in x86
  uint32_t hop_bitmap = status.get_hop_bitmap();
  if (hop_bitmap == 0) {
    return 0;
  }

  // the ideal, and most possible case is that the exact bucket contains it.
  if (bucket.page_id_ == page_id && status.is_content_set() && !status.is_being_modified()) {
    // if the page ID exactly matches, we can assume the offset is valid for a while.
    // but, we have to make sure the cleaner thread is not removing this now.
    // for that, we have to take a fence and then re-check page ID.
    // not acquire. as far as we re-check page ID BEFORE offset, it's safe.
    assorted::memory_fence_consume();
    if (bucket.page_id_ == page_id) {
      return status.get_content_id();
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
    if (status.is_hop_bit_on(i)) {
      const CacheBucket& another_bucket = buckets_[bucket_number + i];
      CacheBucketStatus another_status = another_bucket.status_;  // same as above
      if (another_bucket.page_id_ == page_id
        && another_status.is_content_set()
        && !another_status.is_being_modified()) {
        ContentId content_id = another_status.get_content_id();
        assorted::memory_fence_consume();
        if (another_bucket.page_id_ == page_id) {
          return content_id;
        }
      }
    }
  }
  return 0;
}

inline ErrorCode CacheHashtable::retrieve(
    storage::SnapshotPagePointer page_id,
    ContentId* out,
    PageReadCallback cachemiss_callback,
    void* cachemiss_context) {
  *out = 0;
  ContentId content_id = conservatively_locate(page_id);
  if (content_id != 0) {
    *out = content_id;
    // conservatively_locate() locates the page conservatively, so it might have false negatives
    // (missing concurrently inserted cache page). but it must not have false positives.
    return kErrorCodeOk;
  } else {
    // then we have to install a new entry.
    return install_missed_page(page_id, out, cachemiss_callback, cachemiss_context);
  }
}

}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_HASHTABLE_HPP_
