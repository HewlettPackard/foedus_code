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
#ifndef FOEDUS_EXPERIMENTS_SSSP_HASHTABLE_HPP_
#define FOEDUS_EXPERIMENTS_SSSP_HASHTABLE_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/sssp/sssp_common.hpp"

namespace foedus {
namespace sssp {

/**
 * @brief An in-memory single-threaded data structure to maintain relaxed-nodes for Dijkstra.
 * @details
 * This code is a simplified version of foedus::storage::hash::HashTmpBin.
 *
 * A few major differences (all simplifications):
 * \li No variable-length record. All records are just 16 bytes.
 * \li We thus directly keep the record as hash bucket. No complex memory management.
 * \li No resize
 */
class DijkstraHashtable final {
 public:
  enum Constants {
    /**
     * This is a per-bin data structure. This should be large enough.
     * If not, the bin has many data pages, causing low performance anyways.
     */
    kBucketCount = 1 << 16,
    /**
     * Max number of records to store in this hashtable.
     */
    kMaxRecords = 1 << 16,
  };

  /**
   * Pointer to next record. 0 means null.
   */
  typedef uint16_t NextIndex;

  /** 4-byte node-Id is the unique key. */
  typedef NodeId Key;
  struct Value {
    /** Distance in optimal path from source. 0 means not populated. */
    uint32_t distance_;
    /** Previous node in optimal path from source */
    NodeId previous_;
  };

  /**
   * Represents a record with a unique key.
   * Here, we consume memory super generously. one-page for one record.
   * This makes sure that we don't need any record expansion.
   * We reuse memory for each bin, and we should have at most hundreds of tuples in each bin,
   * so should be fine.
   */
  struct Record {
    Key         key_;     // +4 -> 4
    /** constitutes a singly-linked list in each bucket */
    NextIndex   next_;    // +2 -> 6
    bool        used_;    // +1 -> 7
    uint8_t     padding_;   // +1 -> 8
    Value       value_;   // +8 -> 16

    inline bool is_used() const { return used_; }
    inline void init(Key key) {
      key_ = key;
      next_ = 0;
      used_ = false;
      value_.distance_ = 0;
      value_.previous_ = 0;
    }
  };

  //// Initialization/Uninitialization methods
  /**
   * Constructor doesn't do any initialization.
   * You must call create_memory() before using.
   */
  DijkstraHashtable();

  /**
   * Allocates the memory to use by this object. Must be called before using this object.
   * @param[in] numa_node the NUMA node to allocate this object's memory on
   * @post !memory_->is_null()
   */
  ErrorCode create_memory(uint16_t numa_node);

  /** Destructor automatically releases everything, but you can use this to do it earlier. */
  void      release_memory();

  // steal_memory/give_memory eliminated. We don't need them for this usecase

  /**
   * Clears all data. This assumes there are few records.
   * We don't memset on the whole region, which is expensive.
   */
  void      clean();

  Record*   get_or_create(Key key) ALWAYS_INLINE;
  Record*   get_or_create_follow_next(Key key);

  // The followings assume the key already exists
  Record*   get(Key key) ALWAYS_INLINE;
  Record*   get_follow_next(Key key);

  uint32_t  get_inserted_key_count() const { return inserted_keys_count_; }
  const Key* get_inserted_keys() const { return inserted_keys_; }
  Key* get_inserted_keys() { return inserted_keys_; }

  friend std::ostream& operator<<(std::ostream& o, const DijkstraHashtable& v);

 private:
  /**
   * As a dynamic data structure, we need to allocate memory for tuple data, but we of course
   * can't afford frequent new/malloc (eg std::map). So, we manage memories ourselves.
   * All tuple/bucket data are backed by this memory. Resized automatically.
   */
  memory::AlignedMemory memory_;

  /**
   * buckets_[bucket] is the pointer to head record in the bucket.
   * buckets_[bucket] is null if no entry yet.
   */
  Record*       buckets_;
  /**
   * All keys inserted so far.
   * used only for cleanup.
   */
  Key*          inserted_keys_;
  uint32_t      inserted_keys_count_;

  /**
   * Used only when one bucket receives more than one record.
   */
  Record*       next_records_;
  uint32_t      next_records_count_;

  static uint16_t hashinate(Key key) ALWAYS_INLINE;
};

// see bitswap_dump
const uint8_t kBitSwapArray[] = {
  0, 128, 64, 192, 32, 160, 96, 224, 16, 144, 80, 208, 48, 176, 112, 240,
  8, 136, 72, 200, 40, 168, 104, 232, 24, 152, 88, 216, 56, 184, 120, 248,
  4, 132, 68, 196, 36, 164, 100, 228, 20, 148, 84, 212, 52, 180, 116, 244,
  12, 140, 76, 204, 44, 172, 108, 236, 28, 156, 92, 220, 60, 188, 124, 252,
  2, 130, 66, 194, 34, 162, 98, 226, 18, 146, 82, 210, 50, 178, 114, 242,
  10, 138, 74, 202, 42, 170, 106, 234, 26, 154, 90, 218, 58, 186, 122, 250,
  6, 134, 70, 198, 38, 166, 102, 230, 22, 150, 86, 214, 54, 182, 118, 246,
  14, 142, 78, 206, 46, 174, 110, 238, 30, 158, 94, 222, 62, 190, 126, 254,
  1, 129, 65, 193, 33, 161, 97, 225, 17, 145, 81, 209, 49, 177, 113, 241,
  9, 137, 73, 201, 41, 169, 105, 233, 25, 153, 89, 217, 57, 185, 121, 249,
  5, 133, 69, 197, 37, 165, 101, 229, 21, 149, 85, 213, 53, 181, 117, 245,
  13, 141, 77, 205, 45, 173, 109, 237, 29, 157, 93, 221, 61, 189, 125, 253,
  3, 131, 67, 195, 35, 163, 99, 227, 19, 147, 83, 211, 51, 179, 115, 243,
  11, 139, 75, 203, 43, 171, 107, 235, 27, 155, 91, 219, 59, 187, 123, 251,
  7, 135, 71, 199, 39, 167, 103, 231, 23, 151, 87, 215, 55, 183, 119, 247,
  15, 143, 79, 207, 47, 175, 111, 239, 31, 159, 95, 223, 63, 191, 127, 255,
};

inline uint16_t DijkstraHashtable::hashinate(Key key) {
  uint8_t byte1 = key >> 24;
  uint8_t swapped_byte1 = kBitSwapArray[byte1];
  uint8_t byte2 = key >> 16;
  uint8_t swapped_byte2 = kBitSwapArray[byte2];
  uint16_t swapped_byte12 = (static_cast<uint16_t>(swapped_byte2) << 8) | swapped_byte1;
  uint16_t byte34 = key;
  return swapped_byte12 ^ byte34;
}

inline DijkstraHashtable::Record* DijkstraHashtable::get_or_create(Key key) {
  uint16_t hash = hashinate(key);
  Record* record = buckets_ + hash;
  if (!record->is_used()) {
    // has to create a new record
    record->init(key);
    record->used_ = true;
    inserted_keys_[inserted_keys_count_] = key;
    ++inserted_keys_count_;
    return record;
  }
  if (record->key_ == key) {
    return record;  // found. easy
  }

  // the bucket contains a record, but not this key. collision!
  // this should be rare
  return get_or_create_follow_next(key);
}

inline DijkstraHashtable::Record* DijkstraHashtable::get_or_create_follow_next(Key key) {
  uint16_t hash = hashinate(key);
  Record* record = buckets_ + hash;
  while (true) {
    ASSERT_ND(record->is_used());
    if (record->key_ == key) {
      return record;  // found, okay
    } else if (record->next_ == 0) {
      // we must create a new next record
      const NextIndex next_index = next_records_count_ + 1U;  // because 0 is reserved for null
      Record* new_next = next_records_ + next_index;
      new_next->init(key);
      new_next->used_ = true;
      inserted_keys_[inserted_keys_count_] = key;
      ++inserted_keys_count_;
      record->next_ = next_index;
      ++next_records_count_;
      return new_next;
    }

    Record* next_record = next_records_ + record->next_;
    ASSERT_ND(next_record->is_used());
    // yet another hash collision... seriously?
    record = next_record;
  }
}


inline DijkstraHashtable::Record* DijkstraHashtable::get(Key key) {
  uint16_t hash = hashinate(key);
  Record* record = buckets_ + hash;
  ASSERT_ND(record->is_used());
  if (LIKELY(record->key_ == key)) {
    return record;  // found. easy
  } else {
    return get_follow_next(key);
  }
}

inline DijkstraHashtable::Record* DijkstraHashtable::get_follow_next(Key key) {
  uint16_t hash = hashinate(key);
  Record* record = buckets_ + hash;
  while (true) {
    ASSERT_ND(record->is_used());
    if (record->key_ == key) {
      return record;  // found, okay
    }
    ASSERT_ND(record->next_ != 0);

    Record* next_record = next_records_ + record->next_;
    ASSERT_ND(next_record->is_used());
    // yet another hash collision... seriously?
    record = next_record;
  }
}

}  // namespace sssp
}  // namespace foedus
#endif  // FOEDUS_EXPERIMENTS_SSSP_HASHTABLE_HPP_
