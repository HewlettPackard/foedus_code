/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_INL_HPP_
#define FOEDUS_XCT_XCT_INL_HPP_
#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/assorted/atomic_fences.hpp"

// For log verification. Only in debug mode
#ifndef NDEBUG
#include "foedus/log/log_type_invoke.hpp"
#endif  // NDEBUG

#include "foedus/storage/record.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_access.hpp"

/**
 * @file foedus/xct/xct_inl.hpp
 * @brief Inline functions of Xct.
 * @ingroup XCT
 * @todo these methods are now bigger than before. Maybe don't have to be inlined.
 * Check performance later.
 */
namespace foedus {
namespace xct {

inline ErrorCode Xct::add_to_node_set(
  const storage::VolatilePagePointer* pointer_address,
  storage::VolatilePagePointer observed) {
  ASSERT_ND(!schema_xct_);
  ASSERT_ND(pointer_address);
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  } else if (UNLIKELY(node_set_size_ >= kMaxNodeSets)) {
    return kErrorCodeXctNodeSetOverflow;
  }

  // no need for fence. the observed pointer itself is the only data to verify
  node_set_[node_set_size_].address_ = pointer_address;
  node_set_[node_set_size_].observed_ = observed;
  ++node_set_size_;
  return kErrorCodeOk;
}

inline ErrorCode Xct::add_to_read_set(storage::Storage* storage, storage::Record* record) {
  ASSERT_ND(!schema_xct_);
  ASSERT_ND(storage);
  ASSERT_ND(record);
  if (isolation_level_ == kDirtyReadPreferSnapshot
    || isolation_level_ == kDirtyReadPreferVolatile) {
    return kErrorCodeOk;
  } else if (UNLIKELY(read_set_size_ >= max_read_set_size_)) {
    return kErrorCodeXctReadSetOverflow;
  }

  ASSERT_ND(record->owner_id_.is_valid());

  // If the record is locked, we will surely abort at commit time.
  // Rather, spin here to avoid wasted effort. In our engine, lock happens in commit time,
  // so no worry about deadlock or long wait.
  while (true) {
    read_set_[read_set_size_].observed_owner_id_ = record->owner_id_;
    if (!read_set_[read_set_size_].observed_owner_id_.is_keylocked()) {
      break;
    }
    record->owner_id_.spin_while_keylocked();
  }
  ASSERT_ND(!read_set_[read_set_size_].observed_owner_id_.is_keylocked());

  // for RCU protocol, make sure compiler/CPU don't reorder the data access before tag copy.
  // This is _consume rather than _acquire because it's fine to see stale information as far as
  // we don't access before the tag copy.
  assorted::memory_fence_consume();
  read_set_[read_set_size_].storage_ = storage;
  read_set_[read_set_size_].record_ = record;
  ++read_set_size_;
  return kErrorCodeOk;
}
inline ErrorCode Xct::read_record(storage::Storage* storage, storage::Record* record,
              void *payload, uint16_t payload_offset, uint16_t payload_count) {
  ErrorCode read_set_result = add_to_read_set(storage, record);
  if (read_set_result != kErrorCodeOk) {
    return read_set_result;
  }

  std::memcpy(payload, record->payload_ + payload_offset, payload_count);

  if (isolation_level_ != kDirtyReadPreferSnapshot
    && isolation_level_ != kDirtyReadPreferVolatile) {
    assorted::memory_fence_consume();
    ASSERT_ND(read_set_size_ > 0);
    if (!read_set_[read_set_size_ - 1].observed_owner_id_.equals_all(record->owner_id_)) {
      // this means we might have read something half-updated. abort now.
      return kErrorCodeXctRaceAbort;
    }
  }
  return kErrorCodeOk;
}
template <typename T>
inline ErrorCode Xct::read_record_primitive(storage::Storage* storage, storage::Record* record,
              T *payload, uint16_t payload_offset) {
  ErrorCode read_set_result = add_to_read_set(storage, record);
  if (read_set_result != kErrorCodeOk) {
    return read_set_result;
  }

  char* ptr = record->payload_ + payload_offset;
  *payload = *reinterpret_cast<const T*>(ptr);

  if (isolation_level_ != kDirtyReadPreferSnapshot
    && isolation_level_ != kDirtyReadPreferVolatile) {
    assorted::memory_fence_consume();
    ASSERT_ND(read_set_size_ > 0);
    if (!read_set_[read_set_size_ - 1].observed_owner_id_.equals_all(record->owner_id_)) {
      return kErrorCodeXctRaceAbort;
    }
  }
  return kErrorCodeOk;
}

inline ErrorCode Xct::add_to_write_set(
  storage::Storage* storage,
  storage::Record* record,
  log::RecordLogType* log_entry) {
  ASSERT_ND(!schema_xct_);
  ASSERT_ND(storage);
  ASSERT_ND(record);
  ASSERT_ND(log_entry);
  if (UNLIKELY(write_set_size_ >= max_write_set_size_)) {
    return kErrorCodeXctWriteSetOverflow;
  }

#ifndef NDEBUG
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG

  write_set_[write_set_size_].observed_owner_id_ = record->owner_id_;
  write_set_[write_set_size_].storage_ = storage;
  write_set_[write_set_size_].record_ = record;
  write_set_[write_set_size_].log_entry_ = log_entry;
  ++write_set_size_;
  return kErrorCodeOk;
}

inline ErrorCode Xct::add_to_lock_free_write_set(
  storage::Storage* storage,
  log::RecordLogType* log_entry) {
  ASSERT_ND(!schema_xct_);
  ASSERT_ND(storage);
  ASSERT_ND(log_entry);
  if (UNLIKELY(lock_free_write_set_size_ >= max_lock_free_write_set_size_)) {
    return kErrorCodeXctWriteSetOverflow;
  }

#ifndef NDEBUG
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG

  lock_free_write_set_[lock_free_write_set_size_].storage_ = storage;
  lock_free_write_set_[lock_free_write_set_size_].log_entry_ = log_entry;
  ++lock_free_write_set_size_;
  return kErrorCodeOk;
}

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_INL_HPP_
