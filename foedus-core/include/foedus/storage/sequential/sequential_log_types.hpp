/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_LOG_TYPES_HPP_
#include <stdint.h>

#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/xct/xct_id.hpp"

/**
 * @file foedus/storage/sequential/sequential_log_types.hpp
 * @brief Declares all log types used in this storage type.
 * @ingroup SEQUENTIAL
 */
namespace foedus {
namespace storage {
namespace sequential {
/**
 * @brief Log type of CREATE SEQUENTIAL STORAGE operation.
 * @ingroup SEQUENTIAL LOGTYPE
 * @details
 * This log corresponds to StorageManager::create_sequential() opereation.
 * CREATE STORAGE has no in-epoch transaction order.
 * It is always processed in a separate epoch from operations for the storage.
 * Thus, we advance epoch right after creating a storage (before following operations).
 *
 * This log type is infrequently triggered, so no optimization. All methods defined in cpp.
 */
struct SequentialCreateLogType : public log::StorageLogType {
  LOG_TYPE_NO_CONSTRUCT(SequentialCreateLogType)
  uint16_t        name_length_;       // +2 => 18
  char            name_[6];           // +6 => 24

  static uint16_t calculate_log_length(uint16_t name_length) {
    return assorted::align8(18 + name_length);
  }

  void populate(StorageId storage_id, uint16_t name_length, const char* name);
  void apply_storage(thread::Thread* context, Storage* storage);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const SequentialCreateLogType& v);
};

/**
 * @brief Log type of sequential-storage's append operation.
 * @ingroup ARRAY LOGTYPE
 * @details
 * This is the only record-level operation in sequential storage.
 * It simply appends the the end with an atomic operation.
 */
struct SequentialAppendLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(SequentialAppendLogType)
  uint16_t        payload_count_;     // +2 => 18
  char            payload_[6];        // +6 => 24

  static uint16_t calculate_log_length(uint16_t payload_count) ALWAYS_INLINE {
    // we pad to 8 bytes so that we always have a room for FillerLogType to align.
    return assorted::align8(18 + payload_count);
  }

  void            populate(
    StorageId storage_id,
    const void *payload,
    uint16_t payload_count) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeSequentialAppend;
    header_.log_length_ = calculate_log_length(payload_count);
    header_.storage_id_ = storage_id;
    payload_count_ = payload_count;
    std::memcpy(payload_, payload, payload_count);
  }
  void            apply_record(
    thread::Thread* context,
    Storage* storage,
    xct::XctId* owner_id,
    char* payload) ALWAYS_INLINE {
    // It's a lock-free write set, so it doesn't have record info.
    ASSERT_ND(owner_id == CXX11_NULLPTR);
    ASSERT_ND(payload == CXX11_NULLPTR);
    ASSERT_ND(dynamic_cast<SequentialStorage*>(storage));
    // dynamic_cast is expensive (quite observable in CPU profile)
    // do it only in debug mode. We are sure this is sequential storage (otherwise bug)
    SequentialStorage* casted;
#ifndef NDEBUG
    casted = reinterpret_cast<SequentialStorage*>(storage);
#else  // NDEBUG
    casted = dynamic_cast<SequentialStorage*>(storage);
#endif  // NDEBUG
    ASSERT_ND(casted);
    casted->apply_append_record(context, this);
  }

  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(payload_count_));
    ASSERT_ND(payload_count_ < kMaxPayload);
    ASSERT_ND(header_.get_type() == log::kLogCodeSequentialAppend);
  }

  friend std::ostream& operator<<(std::ostream& o, const SequentialAppendLogType& v);
};

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_LOG_TYPES_HPP_
