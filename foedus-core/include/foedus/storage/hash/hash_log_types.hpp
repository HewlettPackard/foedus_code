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
#ifndef FOEDUS_STORAGE_HASH_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_HASH_LOG_TYPES_HPP_
#include <stdint.h>

#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/xct/xct_id.hpp"

/**
 * @file foedus/storage/hash/hash_log_types.hpp
 * @brief Declares all log types used in this storage type.
 * @ingroup HASH
 */
namespace foedus {
namespace storage {
namespace hash {
/**
 * @brief Log type of CREATE HASH STORAGE operation.
 * @ingroup HASH LOGTYPE
 * @details
 * This log corresponds to StorageManager::create_hash() opereation.
 * CREATE HASH STORAGE has no in-epoch transaction order.
 * It is always processed in a separate epoch from operations for the storage.
 * Thus, we advance epoch right after creating a storage (before following operations).
 *
 * This log type is infrequently triggered, so no optimization. All methods defined in cpp.
 */
struct HashCreateLogType : public log::StorageLogType {
  LOG_TYPE_NO_CONSTRUCT(HashCreateLogType)
  HashMetadata    metadata_;

  void apply_storage(Engine* engine, StorageId storage_id);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const HashCreateLogType& v);
};

/**
 * @brief A base class for HashInsertLogType/HashDeleteLogType/HashOverwriteLogType.
 * @ingroup HASH LOGTYPE
 * @details
 * This defines a common layout for the log types so that composer/partitioner can easier
 * handle these log types. This means we waste a bit (eg delete log type doesn't need payload
 * offset/count), but we anyway have extra space if we want to have data_ 8-byte aligned.
 * data_ always starts with the key, followed by payload for insert/overwrite.
 * @note This data layout is so far compatible even with MasstreeCommonLogType.
 * We might keep this way and reuse some code for masstree package, or maybe not.
 */
struct HashCommonLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(HashCommonLogType)
  uint16_t        key_length_;        // +2 => 18
  uint16_t        payload_offset_;    // +2 => 20
  uint16_t        payload_count_;     // +2 => 22
  uint16_t        reserved_;          // +2 => 24

  /**
   * Full key and (if exists) payload data, both of which are padded to 8 bytes.
   * By padding key part to 8 bytes, slicing becomes more efficient.
   */
  char            aligned_data_[8];   // ~ (+align8(key_length_)+align8(payload_count_))

  static uint16_t calculate_log_length(uint16_t key_length, uint16_t payload_count) ALWAYS_INLINE {
    return 24U + assorted::align8(key_length) + assorted::align8(payload_count);
  }

  char*           get_key() { return aligned_data_; }
  const char*     get_key() const { return aligned_data_; }
  HashValue       calculate_hash() const { return hashinate(get_key(), key_length_); }
  uint16_t        get_key_length_aligned() const { return assorted::align8(key_length_); }
  char*           get_payload() { return aligned_data_ + get_key_length_aligned(); }
  const char*     get_payload() const { return aligned_data_ + get_key_length_aligned(); }
  void            populate_base(
    log::LogCode  type,
    StorageId     storage_id,
    const void*   key,
    uint16_t      key_length,
    const void*   payload = CXX11_NULLPTR,
    uint16_t      payload_offset = 0,
    uint16_t      payload_count = 0) ALWAYS_INLINE {
    header_.log_type_code_ = type;
    header_.log_length_ = calculate_log_length(key_length, payload_count);
    header_.storage_id_ = storage_id;
    key_length_ = key_length;
    payload_offset_ = payload_offset;
    payload_count_ = payload_count;
    reserved_ = 0;

    std::memcpy(aligned_data_, key, key_length);
    uint16_t aligned_key_length = assorted::align8(key_length);
    if (aligned_key_length != key_length) {
      std::memset(aligned_data_ + key_length, 0, aligned_key_length - key_length);
    }
    if (payload_count > 0) {
      uint16_t aligned_payload_count = assorted::align8(payload_count);
      char* payload_base = aligned_data_ + aligned_key_length;
      std::memcpy(payload_base, payload, payload_count);
      if (aligned_payload_count != payload_count) {
        std::memset(payload_base + payload_count, 0, aligned_payload_count - payload_count);
      }
    }
  }

  /** used only for sanity check. checks if the record's and log's keys are equal */
#ifndef NDEBUG
  void assert_record_and_log_keys(xct::LockableXctId* owner_id, const char* data) const {
    const char* log_key = get_key();
    uint16_t log_key_length_aligned = get_key_length_aligned();

    // In HashDataPage::Slot, offset_ etc comes after owner_id. Let's do sanity checks.
    uint16_t* lengthes = reinterpret_cast<uint16_t*>(owner_id + 1);
    // physical length enough long?
    ASSERT_ND(lengthes[1] >= log_key_length_aligned + assorted::align8(payload_count_));
    ASSERT_ND(lengthes[2] == key_length_);  // key length correct?

    // and then HashValue follows.
    ASSERT_ND(hashinate(log_key, key_length_) == reinterpret_cast<HashValue*>(owner_id + 1)[1]);
    // finally, does the key really match?
    ASSERT_ND(std::memcmp(log_key, data, log_key_length_aligned) == 0);
  }
#else  // NDEBUG
  void assert_record_and_log_keys(xct::LockableXctId* /*owner_id*/, const char* /*data*/) const {}
#endif  // NDEBUG

  void assert_type() const ALWAYS_INLINE {
    ASSERT_ND(header_.log_type_code_ == log::kLogCodeHashOverwrite
      || header_.log_type_code_ == log::kLogCodeHashInsert
      || header_.log_type_code_ == log::kLogCodeHashDelete);
  }
};


/**
 * @brief Log type of hash-storage's insert operation.
 * @ingroup HASH LOGTYPE
 * @details
 * Applying this log just flips the delete flag and installs the payload.
 * Allocating the slot and modifying the bloom filter is already done by a system transaction.
 */
struct HashInsertLogType : public HashCommonLogType {
  LOG_TYPE_NO_CONSTRUCT(HashInsertLogType)

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    const void* payload,
    uint16_t    payload_count) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeHashInsert;
    populate_base(type, storage_id, key, key_length, payload, 0, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    ASSERT_ND(owner_id->xct_id_.is_deleted());  // the physical record should be in 'deleted' status
    ASSERT_ND(!owner_id->xct_id_.is_next_layer());
    ASSERT_ND(!owner_id->xct_id_.is_moved());

    // no need to set key in apply(). it's already set when the record is physically inserted
    // (or in other places if this is recovery).
    assert_record_and_log_keys(owner_id, data);

    uint16_t* lengthes = reinterpret_cast<uint16_t*>(owner_id + 1);
    lengthes[3] = payload_count_;  // set payload length

    if (payload_count_ > 0U) {
      // record's payload is also 8-byte aligned, so copy multiply of 8 bytes.
      // if the compiler is smart enough, it will do some optimization here.
      uint16_t key_length_aligned = get_key_length_aligned();
      void* data_payload = ASSUME_ALIGNED(data + key_length_aligned, 8U);
      const void* log_payload = ASSUME_ALIGNED(get_payload(), 8U);
      std::memcpy(data_payload, log_payload, assorted::align8(payload_count_));
    }
    assert_record_and_log_keys(owner_id, data);
    owner_id->xct_id_.set_notdeleted();
  }

  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_, payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeHashInsert);
  }

  friend std::ostream& operator<<(std::ostream& o, const HashInsertLogType& v);
};

/**
 * @brief Log type of hash-storage's delete operation.
 * @ingroup HASH LOGTYPE
 * @details
 * This one does nothing but flipping delete bit.
 */
struct HashDeleteLogType : public HashCommonLogType {
  LOG_TYPE_NO_CONSTRUCT(HashDeleteLogType)

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeHashDelete;
    populate_base(type, storage_id, key, key_length);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    ASSERT_ND(!owner_id->xct_id_.is_deleted());
    ASSERT_ND(!owner_id->xct_id_.is_next_layer());
    ASSERT_ND(!owner_id->xct_id_.is_moved());
    assert_record_and_log_keys(owner_id, data);
    owner_id->xct_id_.set_deleted();
  }

  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_, payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeHashDelete);
  }

  friend std::ostream& operator<<(std::ostream& o, const HashDeleteLogType& v);
};

/**
 * @brief Log type of hash-storage's overwrite operation.
 * @ingroup HASH LOGTYPE
 * @details
 * This is one of the modification operations in hash.
 * It simply invokes memcpy to the payload.
 */
struct HashOverwriteLogType : public HashCommonLogType {
  LOG_TYPE_NO_CONSTRUCT(HashOverwriteLogType)

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    const void* payload,
    uint16_t    payload_offset,
    uint16_t    payload_count) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeHashOverwrite;
    populate_base(type, storage_id, key, key_length, payload, payload_offset, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* data) ALWAYS_INLINE {
    ASSERT_ND(!owner_id->xct_id_.is_deleted());
    ASSERT_ND(!owner_id->xct_id_.is_next_layer());
    ASSERT_ND(!owner_id->xct_id_.is_moved());

    uint16_t key_length_aligned = get_key_length_aligned();
    assert_record_and_log_keys(owner_id, data);

#ifndef NDEBUG
    uint16_t* lengthes = reinterpret_cast<uint16_t*>(owner_id + 1);
    ASSERT_ND(payload_offset_ + payload_count_ <= lengthes[3]);  // aren't we over-running?
#endif  // NDEBUG

    ASSERT_ND(payload_count_ > 0U);
    // Unlike insert, we can't assume 8-bytes alignment because of payload_offset
    std::memcpy(data + key_length_aligned + payload_offset_, get_payload(), payload_count_);
  }

  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_, payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeHashOverwrite);
  }

  friend std::ostream& operator<<(std::ostream& o, const HashOverwriteLogType& v);
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_LOG_TYPES_HPP_
