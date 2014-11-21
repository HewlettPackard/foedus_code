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
#include "foedus/error_stack.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace cache {

// these are actually of the same integer type, but have very different meanings.
/**
 * Offset in hashtable bucket.
 */
typedef uint32_t BucketId;
/**
 * Offset of the actual page image in PagePool for snapshot pages. 0 means not set yet.
 *
 * As each page is 4kb and we expect to spend GBs of DRAM for snapshot cache,
 * the table will have many millions of buckets. However, we can probably assume that
 * the size is within 32 bit. Even with 50% fullness, 2^32 / 2 * 4kb = 8TB.
 * We will not have that much DRAM per NUMA node.
 */
typedef memory::PagePoolOffset ContentId;

/**
 * This is a lossy-compressed representation of SnapshotPagePointer used to quickly identify
 * whether the content of a bucket \e may represent the given SnapshotPagePointer or not.
 * This value is calculated via another simple formula, which should be as independent as possible
 * from hash function (otherwise it can't differentiate entries with similar hash values).
 * This value avoids 0 for sanity checks. If our formula results in 0, we change it to 1.
 */
typedef uint32_t PageIdTag;

/**
 * Position in the overflow buckets. Index-0 is always unused, thus it means null.
 */
typedef uint32_t OverflowPointer;

/**
 * Starting from the given position, we consider this many buckets for the entry.
 * When there is no empty bucket, the entry goes to the overflow linked-list.
 * Setting a larger number means we have to read this many buckets at least for cache-miss case.
 * It is anyway cheap because it's a sequential read, but still must not be too large.
 */
const uint16_t kHopNeighbors = 16U;

/**
 * @brief A simple hash function logic used in snasphot cache.
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
  /** Returns another hash value used to differentiate IDs with similar hash values. */
  static PageIdTag get_tag(storage::SnapshotPagePointer page_id) ALWAYS_INLINE;

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

/**
 * @brief Hash bucket in cache table.
 * @details
 * Every essential information in the hashtable is repsented in this 8-byte.
 * Thus, all simple reads/writes are guaranteed to be complete and consistent (at least regular).
 * This is one of the main tricks to make snapshot cache wait-free.
 *
 * Higher 32 bit is PageIdTag to efficiently check (with false positives) the bucket points to
 * the page or not. Lower 32 bit is ContentId, or the offset in the page pool.
 *
 * We always read-write these information as complete 64bit to allow regular-reads and
 * atomic-writes. We never do atomic operations in this module thanks to the loose requirement in
 * the snapshot cache.
 *
 * But, just be careful on regular-reads and implicitly-atomic-writes.
 * Compiler might do something surprising if we don't explicitly prohibit it.
 * For this purpose, we use a macro equivalent to linux's ACCESS_ONCE. For more details, see:
 *   http://lwn.net/Articles/508991/
 */
struct CacheBucket CXX11_FINAL {
  uint64_t data_;

  ContentId get_content_id() const { return data_; }
  /**
   * @note this operation is not guaranteed to be either atomic or reglar. This should be called
   * only in a local instance of this object, then copied to somewhere as a whole 64bit.
   */
  void      set_content_id(ContentId id) { data_ = (data_ & 0xFFFFFFFF00000000ULL) | id; }
  bool      is_content_set() const { return get_content_id() != 0; }

  PageIdTag get_tag() const { return static_cast<PageIdTag>(data_ >> 32); }
  /** same note as set_content_id() */
  void      set_tag(PageIdTag tag) {
    data_ = (data_ & 0x00000000FFFFFFFFULL) | (static_cast<uint64_t>(tag) << 32);
  }
};

/** A loosely maintained reference count for CLOCK algorithm. */
struct CacheRefCount CXX11_FINAL {
  uint16_t count_;

  void increment() ALWAYS_INLINE {
    if (count_ < 0xFFFFU) {
      ++count_;
    }
  }
  /** returns whether the counter is still non-zero */
  bool decrement(uint16_t subtract) ALWAYS_INLINE {
    if (count_ >= subtract) {
      count_ -= subtract;
    } else {
      count_ = 0;
    }
    return (count_ > 0);
  }
};

/**
 * @brief An entry in the overflow linked list for snapshot cache.
 * @details
 * Entries that didn't find an empty slot are soted in a single linked list.
 * There should be very few entries that go in the linked list. It should be almost empty.
 * As the size of bucket is negligibly smaller than the actual page anyway (8-bytes vs 4kb),
 * we allocate a generous (16x-ish) number of buckets compared to the total count of pages.
 * The hashtable is guaranteed to be very sparse. We don't need anything advanced. Simple stupid.
 */
struct CacheOverflowEntry CXX11_FINAL {
  CacheBucket     bucket_;    // +8 -> 8
  /**
   * Note that we don't have to atomically maintain/follow this pointer thanks to the loose
   * requirements. If we miss an entry, it's just a cache miss. If we couldn't evict some page,
   * it's just one page of wasted DRAM.
   */
  OverflowPointer next_;      // +4 -> 12
  CacheRefCount   refcount_;  // +2 -> 14
  uint16_t        padding_;   // +2 -> 16 (unused)
};

/**
 * @brief A NUMA-local hashtable of cached snapshot pages.
 * @ingroup CACHE
 * @details
 * @par Structure Overview
 * Each key (SnapshotPagePointer) has its own \e best position calculated from its hash value.
 * If the key exists in this table, it is either in some bucket between the best position
 * (including) and best position + kHopNeighbors (excluding) or in the \e overflow linked list.
 * Simple, stupid, thus fast.
 *
 * @par Modularity for Testability
 * Yes, we love them. Classes in this file are totally orthogonal to the actual page pool and
 * other stuffs in the engine. This class only handles \e content (eg offset in page pool)
 * as the data linked to the key. It's left to the caller on how the content
 * is created, consumed, or reclaimed so that we can test/debug/tune the classes easily.
 * In fact, test_hash_table.cpp (the testcase for this class) doesn't even instantiate an engine.
 *
 * @par Hash Function in cache table
 * We have a fixed type of key, SnapshotPagePointer. For the fastest calculation of
 * the hash we simply divide by the size of hashtable, which we adjust to be \e almost a prime
 * number (in many cases actually a prime). We use assorted::ConstDiv to speed up the division.
 *
 * @par Some history on choice of algorithm
 * We were initially based on Herlihy's Hopscotch, but we heavily departed from it
 * to exploit our loose requirements for eliminitating all locks and atomic operations.
 * The only
 * Currently, we even don't do the bucket migration in hopscotch, so it's no longer correct to
 * call this a hop-scotch. Instead, we use an overflow linked list, which should be almost always
 * empty or close-to-empty.
 */
class CacheHashtable CXX11_FINAL {
 public:
  CacheHashtable(BucketId physical_buckets, uint16_t numa_node);

  /**
   * @brief Returns an offset for the given page ID \e opportunistically.
   * @param[in] page_id Page ID to look for
   * @return offset that contains the page. 0 if not found.
   * @details
   * This doesn't take a lock, so a concurrent thread might have inserted the wanted page
   * concurrrently. It's fine. Then we read the page from snapshot, just wasting a bit.
   * Instead, an entry is removed from the cache gracefully, thus an offset we observed
   * will not become invalid soon (pages are garbage collected with grace period).
   * No precise concurrency control needed.
   *
   * However, it might (very occasionally) cause a false negative due to the way we verify
   * (each CacheBucket contains only a compressed version of the page Id).
   * So, the caller must check whether the returned ContentId really points to a correct page,
   * and invoke install() in that case.
   * Again, no precise concurrency control required. Even for false positives/negatives,
   * we just get a bit slower. No correctness issue.
   */
  ContentId find(storage::SnapshotPagePointer page_id) const ALWAYS_INLINE;

  /**
   * @brief Called when a cached page is not found.
   * @return the only possible error code is kErrorCodeCacheTooManyOverflow, which is super-rare.
   * @details
   * This method installs the new content to this hashtable.
   * We are anyway doing at least 4kb memory copy in this case, so no need for serious optimization.
   */
  ErrorCode install(storage::SnapshotPagePointer page_id, ContentId content);

  /** Parameters for evict() */
  struct EvictArgs {
    /** [In] Evicts entries up to about target_count (maybe a bit more or less) */
    uint64_t  target_count_;
    /** [Out] Number of entries that were actually evicted */
    uint64_t  evicted_count_;
    /** [Out] Array of ContentId evicted. */
    ContentId* evicted_contents_;
    // probably will add more in/out parameters to fine-tune its behavior later.

    void add_evicted(ContentId content) {
      // because of the loose synchronization, we might get zero. skip it.
      if (content) {
        evicted_contents_[evicted_count_] = content;
        ++evicted_count_;
      }
    }
  };
  /**
   * @brief Evict some entries from the hashtable.
   * @details
   * Compared to traditional bufferpools, this is much simpler and more scalable thanks to
   * the loose requirements and epoch-based reclamation of the evicted pages.
   * This method only evicts the hashtable entries, so reclaiming the pages pointed from the
   * entries is done by the caller.
   */
  void evict(EvictArgs* args);

  BucketId get_logical_buckets() const ALWAYS_INLINE { return hash_func_.logical_buckets_; }
  BucketId get_physical_buckets() const ALWAYS_INLINE { return hash_func_.physical_buckets_; }

  /**
   * Returns a bucket number the given page ID \e should belong to.
   */
  BucketId get_bucket_number(storage::SnapshotPagePointer page_id) const ALWAYS_INLINE {
    return hash_func_.get_bucket_number(page_id);
  }

  /** only for debugging. don't call this in a race */
  ErrorStack verify_single_thread() const;

  struct Stat {
    uint32_t normal_entries_;
    uint32_t overflow_entries_;
  };
  /** only for debugging. you can call this in a race, but the results are a bit inaccurate. */
  Stat  get_stat_single_thread() const;

  friend std::ostream& operator<<(std::ostream& o, const CacheHashtable& v);

 protected:
  const uint16_t            numa_node_;
  const uint32_t            overflow_buckets_count_;
  const HashFunc            hash_func_;
  memory::AlignedMemory     buckets_memory_;
  memory::AlignedMemory     refcounts_memory_;
  memory::AlignedMemory     overflow_buckets_memory_;

  // these two have the same indexes.
  CacheBucket*              buckets_;
  CacheRefCount*            refcounts_;

  // these are for overflow linked list
  CacheOverflowEntry*       overflow_buckets_;
  /**
   * This forms a singly-linked list of active overflow entries.
   * This is initially 0 (null), and usually remains 0.
   */
  OverflowPointer           overflow_buckets_head_;

  /**
   * This forms another singly-linked list of free overflow entries.
   * A new entry is consumed from the head (by transactions).
   * A returned entry is added back to the head (by cleaner).
   * This overflow free-list is the only data structure we access with atomic operation/mutex.
   * As a new entry is added to overflow list very occasionally, this should be fine.
   */
  OverflowPointer           overflow_free_buckets_head_;

  /**
   * The mutex to protect free overflow entries.
   * Actually this is not a shared mutex, but we reuse the class to reduce code.
   */
  soc::SharedMutex          overflow_free_buckets_mutex_;

  /**
   * We previously stopped eviction here for usual buckets.
   * We will resume from this number +1 next time. We will then sequetnially check.
   * The overflow list is fully checked after each wrap-around of this clockhand,
   * so we don't have a dedicated clock hand for overflow list.
   */
  BucketId                  clockhand_;

  BucketId  evict_main_loop(EvictArgs* args, BucketId cur, uint16_t loop);
  void      evict_overflow_loop(EvictArgs* args, uint16_t loop);
};

inline uint32_t HashFunc::get_hash(storage::SnapshotPagePointer page_id) {
  uint16_t snapshot_id = storage::extract_snapshot_id_from_snapshot_pointer(page_id);
  uint8_t numa_node = storage::extract_numa_node_from_snapshot_pointer(page_id);
  uint64_t local_page_id = storage::extract_local_page_id_from_snapshot_pointer(page_id);
  // snapshot_id is usually very sparse. and has no correlation with local_page_id.
  // numa_node too. thus just use them as seeds for good old multiplicative hashing.
  uint32_t seed = snapshot_id * 0x5a2948074497175aULL;
  seed += numa_node * 0xc30a95f6e63dd908ULL;

  // local_page_id is 40 bits. 32-40 bits are very sparse, and it has no correlation with
  // other bits (how come 4kb-th page and 16Tb+4kb-th page has anything in common..).
  // so, let's treat it with another multiplicative.
  uint8_t highest_bits = static_cast<uint8_t>(local_page_id >> 32);
  uint32_t folded_local_page_id = local_page_id + highest_bits * 0xd3561f5bedac324cULL;

  // now, we don't want to place adjacent pages in adjacent hash buckets. so, let's swap bytes.
  // this benefits hop-scotch hashing table.
  uint32_t swaped_page_id = __builtin_bswap32(folded_local_page_id);
  seed ^= swaped_page_id;
  return seed;
}

inline PageIdTag HashFunc::get_tag(storage::SnapshotPagePointer page_id) {
  PageIdTag high_bits = static_cast<PageIdTag>(page_id >> 32);
  PageIdTag tag = high_bits * 0x8e1d76486c3e638dULL + static_cast<PageIdTag>(page_id);
  if (tag == 0) {
    tag = 1;  // we avoid 0 as a valid value. this enables sanity checks.
  }
  return tag;
}

inline ContentId CacheHashtable::find(storage::SnapshotPagePointer page_id) const {
  ASSERT_ND(page_id > 0);
  BucketId bucket_number = get_bucket_number(page_id);
  ASSERT_ND(bucket_number < get_logical_buckets());

  // we prefetch up to 128 bytes (16 entries).
  assorted::prefetch_cachelines(buckets_ + bucket_number, 2);

  PageIdTag tag = HashFunc::get_tag(page_id);
  ASSERT_ND(tag != 0);
  for (uint16_t i = 0; i < kHopNeighbors; ++i) {
    const CacheBucket& bucket = buckets_[bucket_number + i];
    if (bucket.get_tag() == tag) {
      // found (probably)!
      refcounts_[bucket_number + i].increment();
      return bucket.get_content_id();
    }
  }

  // Not found. let's check overflow list
  if (overflow_buckets_head_) {
    for (OverflowPointer i = overflow_buckets_head_; i != 0;) {
      if (overflow_buckets_[i].bucket_.get_tag() == tag) {
        overflow_buckets_[i].refcount_.increment();
        return overflow_buckets_[i].bucket_.get_content_id();
      }
      i = overflow_buckets_[i].next_;
    }
  }

  return 0;
}

}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_HASHTABLE_HPP_
