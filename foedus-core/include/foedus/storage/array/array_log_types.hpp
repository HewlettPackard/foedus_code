/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_ARRAY_LOG_TYPES_HPP_
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
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

/**
 * @file foedus/storage/array/array_log_types.hpp
 * @brief Declares all log types used in this storage type.
 * @ingroup ARRAY
 */
namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Log type of CREATE ARRAY STORAGE operation.
 * @ingroup ARRAY LOGTYPE
 * @details
 * This log corresponds to StorageManager::create_array() opereation.
 * CREATE ARRAY STORAGE has no in-epoch transaction order.
 * It is always processed in a separate epoch from operations for the storage.
 * Thus, we advance epoch right after creating a storage (before following operations).
 *
 * This log type is infrequently triggered, so no optimization. All methods defined in cpp.
 */
struct CreateLogType : public log::StorageLogType {
  LOG_TYPE_NO_CONSTRUCT(CreateLogType)
  ArrayOffset     array_size_;        // +8 => 24
  uint16_t        payload_size_;      // +2 => 26
  uint16_t        name_length_;       // +2 => 28
  char            name_[4];           // +4 => 32

  static uint16_t calculate_log_length(uint16_t name_length) {
    return assorted::align8(28 + name_length);
  }

  void populate(StorageId storage_id, ArrayOffset array_size,
      uint16_t payload_size, uint16_t name_length, const char* name);
  void apply_storage(thread::Thread* context, Storage* storage);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const CreateLogType& v);
};

/**
 * @brief Log type of array-storage's overwrite operation.
 * @ingroup ARRAY LOGTYPE
 * @details
 * This is the only modification operation in array.
 * It simply invokes memcpy to the payload.
 */
struct OverwriteLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(OverwriteLogType)
  ArrayOffset     offset_;            // +8 => 24
  uint16_t        payload_offset_;    // +2 => 26
  uint16_t        payload_count_;     // +2 => 28
  char            payload_[4];        // +4 => 32

  static uint16_t calculate_log_length(uint16_t payload_count) ALWAYS_INLINE {
    // we pad to 8 bytes so that we always have a room for FillerLogType to align.
    return assorted::align8(28 + payload_count);
  }

  void            populate(StorageId storage_id, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeArrayOverwrite;
    header_.log_length_ = calculate_log_length(payload_count);
    header_.storage_id_ = storage_id;
    offset_ = offset;
    payload_offset_ = payload_offset;
    payload_count_ = payload_count;
    std::memcpy(payload_, payload, payload_count);
  }
  /** For primitive types. A bit more efficient. */
  template <typename T>
  void            populate_primitive(StorageId storage_id,
      ArrayOffset offset, T payload, uint16_t payload_offset) {
    header_.log_type_code_ = log::kLogCodeArrayOverwrite;
    header_.log_length_ = calculate_log_length(sizeof(T));
    header_.storage_id_ = storage_id;
    offset_ = offset;
    payload_offset_ = payload_offset;
    payload_count_ = sizeof(T);
    T* address = reinterpret_cast<T*>(payload_);
    *address = payload;
  }
  void            apply_record(thread::Thread* /*context*/,
                               Storage* storage, Record* record) ALWAYS_INLINE {
    ASSERT_ND(payload_count_ < kDataSize);
    ASSERT_ND(dynamic_cast<ArrayStorage*>(storage));
    std::memcpy(record->payload_ + payload_offset_, payload_, payload_count_);
    assorted::memory_fence_release();  // we must apply BEFORE unlock
  }

  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeArrayOverwrite);
  }

  friend std::ostream& operator<<(std::ostream& o, const OverwriteLogType& v);
};

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_LOG_TYPES_HPP_
