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
struct ArrayCreateLogType : public log::StorageLogType {
  LOG_TYPE_NO_CONSTRUCT(ArrayCreateLogType)
  ArrayOffset     array_size_;        // +8 => 24
  uint16_t        payload_size_;      // +2 => 26
  uint16_t        name_length_;       // +2 => 28
  char            name_[4];           // +4 => 32

  static uint16_t calculate_log_length(uint16_t name_length) {
    return assorted::align8(28 + name_length);
  }

  void populate(StorageId storage_id, ArrayOffset array_size,
      uint16_t payload_size, uint16_t name_length, const char* name);
  static void construct(const Metadata* metadata, void* buffer);
  void apply_storage(Engine* engine, StorageId storage_id);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const ArrayCreateLogType& v);
};

/**
 * @brief A base class for ArrayOverwriteLogType/ArrayIncrementLogType.
 * @ingroup ARRAY LOGTYPE
 * @details
 * This just defines ArrayOffset as the first data. We use this class only where we just
 * need to access the array offset.
 */
struct ArrayCommonUpdateLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(ArrayCommonUpdateLogType)
  ArrayOffset     offset_;            // +8 => 24
  // payload_offset_ is also a common property, but we can't include it here for alignment.
};

/**
 * @brief Log type of array-storage's overwrite operation.
 * @ingroup ARRAY LOGTYPE
 * @details
 * This is a modification operation in array.
 * It simply invokes memcpy to the payload.
 */
struct ArrayOverwriteLogType : public ArrayCommonUpdateLogType {
  LOG_TYPE_NO_CONSTRUCT(ArrayOverwriteLogType)
  uint16_t        payload_offset_;    // +2 => 26
  uint16_t        payload_count_;     // +2 => 28
  char            payload_[4];        // +4 => 32

  static uint16_t calculate_log_length(uint16_t payload_count) ALWAYS_INLINE {
    // we pad to 8 bytes so that we always have a room for FillerLogType to align.
    return assorted::align8(28 + payload_count);
  }

  void populate(
    StorageId storage_id,
    ArrayOffset offset,
    const void *payload,
    uint16_t payload_offset,
    uint16_t payload_count) ALWAYS_INLINE;

  /** For primitive types. A bit more efficient. */
  template <typename T>
  void populate_primitive(
    StorageId storage_id,
    ArrayOffset offset,
    T payload,
    uint16_t payload_offset) ALWAYS_INLINE;

  void apply_record(
    thread::Thread* context,
    StorageId storage_id,
    xct::LockableXctId* owner_id,
    char* payload) const ALWAYS_INLINE;

  void assert_valid() const ALWAYS_INLINE;

  friend std::ostream& operator<<(std::ostream& o, const ArrayOverwriteLogType& v);
};

/** Used in ArrayIncrementLogType. */
enum ValueType {
  kUnknown = 0,
  kI8 = 1,
  kI16,
  kI32,
  kU8,
  kU16,
  kU32,
  kFloat,
  kBool,
  // above are 32bits or less, below are 64 bits
  kI64,
  kU64,
  kDouble,
};
template <typename T> ValueType to_value_type();
template <> inline ValueType to_value_type<bool>() { return kBool; }
template <> inline ValueType to_value_type<int8_t>() { return kI8; }
template <> inline ValueType to_value_type<int16_t>() { return kI16; }
template <> inline ValueType to_value_type<int32_t>() { return kI32; }
template <> inline ValueType to_value_type<int64_t>() { return kI64; }
template <> inline ValueType to_value_type<uint8_t>() { return kU8; }
template <> inline ValueType to_value_type<uint16_t>() { return kU16; }
template <> inline ValueType to_value_type<uint32_t>() { return kU32; }
template <> inline ValueType to_value_type<uint64_t>() { return kU64; }
template <> inline ValueType to_value_type<float>() { return kFloat; }
template <> inline ValueType to_value_type<double>() { return kDouble ; }

/**
 * @brief Log type of array-storage's increment operation.
 * @ingroup ARRAY LOGTYPE
 * @details
 * This is similar to overwrite, but different in a sense that this can do value-increment
 * without relying on the current value.
 * For that, we remember the addendum in primitive format.
 */
struct ArrayIncrementLogType : public ArrayCommonUpdateLogType {
  LOG_TYPE_NO_CONSTRUCT(ArrayIncrementLogType)
  uint16_t        payload_offset_;    // +2 => 26
  uint16_t        value_type_;        // +2 => 28
  char            addendum_[4];       // +4 => 32

  static uint16_t calculate_log_length(ValueType value_type) ALWAYS_INLINE {
    if (value_type < kI64) {
      return 32;  // in this case we store it in first bytes of addendum
    } else {
      return 40;  // in this case we store it in 32th-bytes (28-32th bytes are not used)
    }
  }

  template <typename T>
  void populate(
    StorageId storage_id,
    ArrayOffset offset,
    T payload,
    uint16_t payload_offset) ALWAYS_INLINE;

  ValueType get_value_type() const ALWAYS_INLINE { return static_cast<ValueType>(value_type_); }
  bool is_64b_type() const ALWAYS_INLINE { return get_value_type() >= kI64; }
  void*       addendum_64() { return addendum_ + 4; }
  const void* addendum_64() const { return addendum_ + 4; }

  void apply_record(
    thread::Thread* context,
    StorageId storage_id,
    xct::LockableXctId* owner_id,
    char* payload) const ALWAYS_INLINE;

  /**
   * A special optimization for increment logs in log gleaner.
   * Two increment logs on the same array offset can be merged to reduce # of log entries.
   * @pre storage_id_ == other.storage_id_
   * @pre value_type_ == other.value_type_
   * @pre payload_offset_ == other.payload_offset_
   */
  void merge(const ArrayIncrementLogType& other) ALWAYS_INLINE;

  void assert_valid() const ALWAYS_INLINE;

  friend std::ostream& operator<<(std::ostream& o, const ArrayIncrementLogType& v);
};


inline void ArrayOverwriteLogType::populate(
  StorageId storage_id,
  ArrayOffset offset,
  const void *payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  header_.log_type_code_ = log::kLogCodeArrayOverwrite;
  header_.log_length_ = calculate_log_length(payload_count);
  header_.storage_id_ = storage_id;
  offset_ = offset;
  payload_offset_ = payload_offset;
  payload_count_ = payload_count;
  std::memcpy(payload_, payload, payload_count);
}

template <typename T>
inline void ArrayOverwriteLogType::populate_primitive(
  StorageId storage_id,
  ArrayOffset offset,
  T payload,
  uint16_t payload_offset) {
  header_.log_type_code_ = log::kLogCodeArrayOverwrite;
  header_.log_length_ = calculate_log_length(sizeof(T));
  header_.storage_id_ = storage_id;
  offset_ = offset;
  payload_offset_ = payload_offset;
  payload_count_ = sizeof(T);
  T* address = reinterpret_cast<T*>(payload_);
  *address = payload;
}

inline void ArrayOverwriteLogType::apply_record(
  thread::Thread* /*context*/,
  StorageId /*storage_id*/,
  xct::LockableXctId* /*owner_id*/,
  char* payload) const {
  ASSERT_ND(payload_count_ < kDataSize);
  std::memcpy(payload + payload_offset_, payload_, payload_count_);
}

inline void ArrayOverwriteLogType::assert_valid() const {
  assert_valid_generic();
  ASSERT_ND(header_.log_length_ == calculate_log_length(payload_count_));
  ASSERT_ND(header_.get_type() == log::kLogCodeArrayOverwrite);
}

template <typename T>
inline void ArrayIncrementLogType::populate(
  StorageId storage_id,
  ArrayOffset offset,
  T payload,
  uint16_t payload_offset) {
  header_.log_type_code_ = log::kLogCodeArrayIncrement;
  ValueType type = to_value_type<T>();
  header_.log_length_ = calculate_log_length(type);
  header_.storage_id_ = storage_id;
  offset_ = offset;
  payload_offset_ = payload_offset;
  value_type_ = type;
  if (is_64b_type()) {
    T* address = reinterpret_cast<T*>(addendum_ + 4);
    *address = payload;
  } else {
    T* address = reinterpret_cast<T*>(addendum_);
    *address = payload;
  }
}

template <typename T>
inline void increment(T* payload, const T* addendum) {
  *payload += *addendum;
}

inline void ArrayIncrementLogType::apply_record(
  thread::Thread* /*context*/,
  StorageId /*storage_id*/,
  xct::LockableXctId* /*owner_id*/,
  char* payload) const {
  switch (get_value_type()) {
    // 32 bit data types
    case kI8:
      increment<int8_t>(
        reinterpret_cast<int8_t*>(payload + payload_offset_),
        reinterpret_cast<const int8_t*>(addendum_));
      break;
    case kI16:
      increment<int16_t>(
        reinterpret_cast<int16_t*>(payload + payload_offset_),
        reinterpret_cast<const int16_t*>(addendum_));
      break;
    case kI32:
      increment<int32_t>(
        reinterpret_cast<int32_t*>(payload + payload_offset_),
        reinterpret_cast<const int32_t*>(addendum_));
      break;
    case kBool:
    case kU8:
      increment<uint8_t>(
        reinterpret_cast<uint8_t*>(payload + payload_offset_),
        reinterpret_cast<const uint8_t*>(addendum_));
      break;
    case kU16:
      increment<uint16_t>(
        reinterpret_cast<uint16_t*>(payload + payload_offset_),
        reinterpret_cast<const uint16_t*>(addendum_));
      break;
    case kU32:
      increment<uint32_t>(
        reinterpret_cast<uint32_t*>(payload + payload_offset_),
        reinterpret_cast<const uint32_t*>(addendum_));
      break;
    case kFloat:
      increment<float>(
        reinterpret_cast<float*>(payload + payload_offset_),
        reinterpret_cast<const float*>(addendum_));
      break;

    // 64 bit data types
    case kI64:
      increment<int64_t>(
        reinterpret_cast<int64_t*>(payload + payload_offset_),
        reinterpret_cast<const int64_t*>(addendum_ + 4));
      break;
    case kU64:
      increment<uint64_t>(
        reinterpret_cast<uint64_t*>(payload + payload_offset_),
        reinterpret_cast<const uint64_t*>(addendum_ + 4));
      break;
    case kDouble:
      increment<double>(
        reinterpret_cast<double*>(payload + payload_offset_),
        reinterpret_cast<const double*>(addendum_ + 4));
      break;
    default:
      ASSERT_ND(false);
      break;
  }
}

template <typename T>
inline void add_to(void* destination, const void* added) {
  *(reinterpret_cast< T* >(destination)) += *(reinterpret_cast< const T* >(added));
}

inline void ArrayIncrementLogType::merge(const ArrayIncrementLogType& other) {
  ASSERT_ND(header_.storage_id_ == other.header_.storage_id_);
  ASSERT_ND(value_type_ == other.value_type_);
  ASSERT_ND(payload_offset_ == other.payload_offset_);
  switch (get_value_type()) {
    // 32 bit data types
    case kI8:
      add_to<int8_t>(addendum_, other.addendum_);
      break;
    case kI16:
      add_to<int16_t>(addendum_, other.addendum_);
      break;
    case kI32:
      add_to<int32_t>(addendum_, other.addendum_);
      break;
    case kBool:
    case kU8:
      add_to<uint8_t>(addendum_, other.addendum_);
      break;
    case kU16:
      add_to<uint16_t>(addendum_, other.addendum_);
      break;
    case kU32:
      add_to<uint32_t>(addendum_, other.addendum_);
      break;
    case kFloat:
      add_to<float>(addendum_, other.addendum_);
      break;

    // 64 bit data types
    case kI64:
      add_to<int64_t>(addendum_64(), other.addendum_64());
      break;
    case kU64:
      add_to<uint64_t>(addendum_64(), other.addendum_64());
      break;
    case kDouble:
      add_to<double>(addendum_64(), other.addendum_64());
      break;
    default:
      ASSERT_ND(false);
      break;
  }
}

inline void ArrayIncrementLogType::assert_valid() const {
  assert_valid_generic();
  ASSERT_ND(header_.log_length_ == calculate_log_length(get_value_type()));
  ASSERT_ND(header_.get_type() == log::kLogCodeArrayIncrement);
  ASSERT_ND(get_value_type() >= kI8 && get_value_type() <= kDouble);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_LOG_TYPES_HPP_
