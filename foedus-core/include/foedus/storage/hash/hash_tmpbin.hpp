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
#ifndef FOEDUS_STORAGE_HASH_HASH_TMPBIN_HPP_
#define FOEDUS_STORAGE_HASH_HASH_TMPBIN_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief An in-memory single-threaded data structure to compose tuples in a hash bin.
 * @ingroup HASH
 * @details
 * This object is used in the hash composer to organize data pages for each hash bin, applying
 * log entries to existing tuples.
 * In essense, it is another hash-table where bucket is low-bits (while bin-bits use high-bits).
 * For testability and modularity, we separated this to its own class.
 *
 * @par Access pattern to optimize for
 * Reducer sorts hash log entries by their hash bins. In each bin, logs are sorted by timestamp.
 * So, the inputs are not sorted by full-hash values, which would have simplified how this object
 * indexes each tuple (just linked list is enough).
 * Instead, we can assume that the next access on the same key surely has a larger timestamp,
 * so this object doesn't have to worry about the order of applying modifications.
 */
class HashTmpBin CXX11_FINAL {
 public:
  enum Constants {
    /**
     * This is a per-bin data structure. This should be large enough.
     * If not, the bin has many data pages, causing low performance anyways.
     */
    kBucketCount = 1 << 16,
    /** Let's be super generous, assuming that we are reusing memories */
    kDefaultInitialSize = 1 << 24,
  };

  /**
   * Pointer to Record. 0 means null.
   */
  typedef uint32_t RecordIndex;

  /**
   * Represents a record with a unique key.
   * Here, we consume memory super generously. one-page for one record.
   * This makes sure that we don't need any record expansion.
   * We reuse memory for each bin, and we should have at most hundreds of tuples in each bin,
   * so should be fine.
   */
  struct Record {
    xct::XctId  xct_id_;          // +8 -> 8
    uint16_t    key_length_;      // +2 -> 10
    uint16_t    payload_length_;  // +2 -> 12
    uint16_t    aligned_key_length_;      // +2 -> 14
    uint16_t    aligned_payload_length_;  // +2 -> 16
    HashValue   hash_;            // +8 -> 24
    /** constitutes a singly-linked list in each bucket */
    RecordIndex next_;            // +4 -> 28
    uint32_t    unused_;          // +4 -> 32
    /** key and payload, like the log entry, key part is 8-byte aligned. */
    char        data_[kPageSize - 32];  // -> kPageSize

    inline char*  get_key() ALWAYS_INLINE { return data_; }
    inline char*  get_payload() ALWAYS_INLINE { return data_ + aligned_key_length_; }
    inline Record*      get_record_next(const HashTmpBin* enclosure) const ALWAYS_INLINE {
      return enclosure->get_record(next_);
    }

    inline void   set_all(
      xct::XctId xct_id,
      const void* key,
      uint16_t key_length,
      HashValue hash,
      const void* payload,
      uint16_t payload_length) ALWAYS_INLINE {
      xct_id_ = xct_id;
      ASSERT_ND(!xct_id.is_deleted());
      key_length_ = key_length;
      payload_length_ = payload_length;
      aligned_key_length_ = assorted::align8(key_length);
      aligned_payload_length_ = assorted::align8(payload_length);
      hash_ = hash;
      next_ = 0;

      std::memcpy(get_key(), key, key_length);
      if (key_length != aligned_key_length_) {
        std::memset(get_key() + key_length, 0, aligned_key_length_ - key_length);
      }

      std::memcpy(get_payload(), payload, payload_length);
      if (payload_length != aligned_payload_length_) {
        std::memset(get_payload() + payload_length, 0, aligned_payload_length_ - payload_length);
      }
    }

    inline void   set_payload(const void* payload, uint16_t payload_length) ALWAYS_INLINE {
      payload_length_ = payload_length;
      aligned_payload_length_ = assorted::align8(payload_length);

      std::memcpy(get_payload(), payload, payload_length);
      if (payload_length != aligned_payload_length_) {
        std::memset(get_payload() + payload_length, 0, aligned_payload_length_ - payload_length);
      }
    }
    inline void   overwrite_payload(
      const void* payload,
      uint16_t payload_offset,
      uint16_t payload_count) ALWAYS_INLINE {
      std::memcpy(get_payload() + payload_offset, payload, payload_count);
    }
  };

  //// Initialization/Uninitialization methods
  /**
   * Constructor doesn't do any initialization.
   * You must call create_memory() before using.
   */
  HashTmpBin();

  /**
   * Allocates the memory to use by this object. Must be called before using this object.
   * @param[in] numa_node the NUMA node to allocate this object's memory on
   * @param[in] initial_size byte size of the memory to start from.
   * @pre initial_size > sizeof(RecordIndex)*kBucketCount, in reality should be much larger
   * @post !memory_->is_null()
   */
  ErrorCode create_memory(uint16_t numa_node, uint64_t initial_size = kDefaultInitialSize);

  /** Destructor automatically releases everything, but you can use this to do it earlier. */
  void      release_memory();

  /**
   * This is a special version of create_memory() where this object \e steals the memory ownership
   * from the recipient. Combined with give_memory(), this allows completely reusing memory.
   * @param[in,out] provider the memory from which this object steals ownership
   * @pre !provider->is_null()
   * @post provider->is_null()
   * @post !memory_->is_null()
   */
  void      steal_memory(memory::AlignedMemory* provider);

  /**
   * This is a special version of release_memory() where this object \e moves the memory ownership
   * to the recipient. Combined with steal_memory(), this allows completely reusing memory.
   * @param[in,out] recipient the memory to receive the ownership of memory this object has
   * @pre !memory_->is_null()
   * @post !recipient->is_null()
   * @post memory_->is_null()
   */
  void      give_memory(memory::AlignedMemory* recipient);

  /**
   * Removes all tuple data for the current bin.
   * The memory is kept so that we can efficiently reuse resources for next bins to process.
   *
   * This is a \e full cleaning method that can be used anytime, but costs a bit more.
   * When there were few records, it is a bit expensive to mem-zero the entire buckets_.
   * In that case, use clean_quick()
   */
  void      clean();
  /**
   * This version selectively clears buckets_ by seeing individual records.
   * Hence, this can be used only after records_consumed_ is populated.
   * Instead, when there are just a few records, this is much faster than clean().
   */
  void      clean_quick();

  //// Record-access methods
  Record*     get_record(RecordIndex index) const {
    ASSERT_ND(index < records_capacity_);
    ASSERT_ND(index >= sizeof(RecordIndex) * kBucketCount / sizeof(Record));
    return records_ + index;
  }
  RecordIndex get_records_capacity() const { return records_capacity_; }
  RecordIndex get_records_consumed() const { return records_consumed_; }
  RecordIndex get_first_record() const {
    return sizeof(RecordIndex) * kBucketCount / sizeof(Record);
  }
  /** @returns number of physical records, which may or may not be logically deleted */
  uint32_t    get_physical_record_count() const {
    return get_records_consumed() - get_first_record();
  }
  RecordIndex get_bucket_head(uint32_t bucket) const {
    ASSERT_ND(bucket < kBucketCount);
    return buckets_[bucket];
  }

  //// Data manipulation methods

  /**
   * @brief Inserts a new record of the given key and payload.
   * @details
   * If there is an existing record of the key that is logically deleted, we flip the deletion flag.
   * If such an record exists and also is not logically deleted, this method returns an error
   * (kErrorCodeStrKeyAlreadyExists). Considering how this object is used, it mustn't hapepn.
   */
  ErrorCode insert_record(
    xct::XctId xct_id,
    const void* key,
    uint16_t key_length,
    HashValue hash,
    const void* payload,
    uint16_t payload_length);

  /**
   * @brief Logically deletes a record of the given key.
   * @details
   * If there is no existing record of the key, or such a record is already logically deleted,
   * this method returns an error (kErrorCodeStrKeyNotFound). Mustn't happen either.
   */
  ErrorCode delete_record(
    xct::XctId xct_id,
    const void* key,
    uint16_t key_length,
    HashValue hash);

  /**
   * @brief Overwrites a part of the record of the given key.
   * @details
   * If there is no existing record of the key, or such a record is already logically deleted,
   * this method returns an error (kErrorCodeStrKeyNotFound). Mustn't happen either.
   */
  ErrorCode overwrite_record(
    xct::XctId xct_id,
    const void* key,
    uint16_t key_length,
    HashValue hash,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /**
   * @brief Updates a record of the given key with the given payload, which might change length.
   * @details
   * This method is same as delete_record()+insert_record().
   * The difference is that this can be faster because it does only one lookup.
   * If there is no existing record of the key, or such a record is already logically deleted,
   * this method returns an error (kErrorCodeStrKeyNotFound). Mustn't happen either.
   */
  ErrorCode update_record(
    xct::XctId xct_id,
    const void* key,
    uint16_t key_length,
    HashValue hash,
    const void* payload,
    uint16_t payload_length);

  friend std::ostream& operator<<(std::ostream& o, const HashTmpBin& v);

 private:
  /**
   * As a dynamic data structure, we need to allocate memory for tuple data, but we of course
   * can't afford frequent new/malloc (eg std::map). So, we manage memories ourselves.
   * All tuple/bucket data are backed by this memory. Resized automatically.
   */
  memory::AlignedMemory memory_;

  /**
   * RecordIndex of this value or larger exceeds memory_.
   * When records_consumed_ reaches this value, we must resize the memory.
   */
  RecordIndex           records_capacity_;
  /**
   * How many . This is purely increasing.
   * We don't do any garbage collection. Once we are done with the current hash bin,
   * we just reset this value, and zero-claer buckets_.
   */
  RecordIndex           records_consumed_;

  /**
   * buckets_[bucket] is the pointer to head record in the bucket.
   * buckets_[bucket] is null if no entry yet.
   */
  RecordIndex*          buckets_;

  /**
   * Array of record data backed by memory_. Index is RecordIndex.
   * This points to the same memory as buckets_. We just never use an index less than
   * sizeof(RecordIndex) * kBucketCount / sizeof(Record).
   */
  Record*               records_;

  /** Allocate a memory region for a new record. Resize the memory if needed. */
  ErrorCode               alloc_record(RecordIndex* out) ALWAYS_INLINE;
  /** Whenever memory_ variable is changed/resized, this function is fired. */
  void                    on_memory_set();

  /** @returns index in buckets_ corresponding to the given hash value */
  inline static uint16_t  extract_bucket(HashValue hash) ALWAYS_INLINE {
    return hash & 0xFFFFU;  // so far the lowest 16 bits.
  }

  /** Result of search_bucket() */
  struct SearchResult {
    SearchResult(RecordIndex found, RecordIndex tail) : found_(found), tail_(tail) {}
    /** If found, the record for the key. 0 otherwise */
    RecordIndex found_;
    /** If not found, this tells the last entry in the bucket (0 if the bucket is empty). */
    RecordIndex tail_;
  };
  SearchResult search_bucket(
    const void* key,
    uint16_t key_length,
    HashValue hash) const ALWAYS_INLINE;
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_TMPBIN_HPP_
