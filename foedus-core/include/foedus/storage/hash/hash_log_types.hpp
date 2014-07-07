/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_HASH_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_HASH_LOG_TYPES_HPP_
#include <stdint.h>

#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
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
  uint16_t        name_length_;       // +2 => 18
  char            name_[6];           // +6 => 24

  static uint16_t calculate_log_length(uint16_t name_length) {
    return assorted::align8(18 + name_length);
  }

  void populate(StorageId storage_id, uint16_t name_length, const char* name);
  void apply_storage(thread::Thread* context, Storage* storage);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const HashCreateLogType& v);
};

/**
 * @brief Log type of hash-storage's overwrite operation.
 * @ingroup HASH LOGTYPE
 * @details
 * This is the only modification operation in hash.
 * It simply invokes memcpy to the payload.
 */
struct HashOverwriteLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(HashOverwriteLogType)
  uint16_t        key_length_;        // +2 => 18
  uint16_t        payload_offset_;    // +2 => 20
  uint16_t        payload_count_;     // +2 => 22
  char            data_[2];           // +2 => 24

  static uint16_t calculate_log_length(uint16_t key_length, uint16_t payload_count) ALWAYS_INLINE {
    // we pad to 8 bytes so that we always have a room for FillerLogType to align.
    return assorted::align8(22 + key_length + payload_count);
  }

  void            populate(StorageId storage_id, uint16_t key_length, const void *key,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeHashOverwrite;
    header_.log_length_ = calculate_log_length(key_length, payload_count);
    header_.storage_id_ = storage_id;
    key_length_ = key_length;
    payload_offset_ = payload_offset;
    payload_count_ = payload_count;
    std::memcpy(data_, key, key_length);
    std::memcpy(data_ + key_length_, payload, payload_count);
  }
  
  void            apply_record(thread::Thread* /*context*/,
                               Storage* storage, Record* record) ALWAYS_INLINE {
//    ASSERT_ND(payload_count_ < kDataSize);
//    ASSERT_ND(dynamic_cast<HashStorage*>(storage));
    std::memcpy(record->payload_ + payload_offset_, data_ + key_length_, payload_count_);
    assorted::memory_fence_release();  // we must apply BEFORE unlock
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
