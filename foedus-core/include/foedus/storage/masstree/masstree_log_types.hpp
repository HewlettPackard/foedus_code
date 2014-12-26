/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_LOG_TYPES_HPP_
#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <iosfwd>

#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"

/**
 * @file foedus/storage/masstree/masstree_log_types.hpp
 * @brief Declares all log types used in this storage type.
 * @ingroup MASSTREE
 */
namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Log type of CREATE MASSTREE STORAGE operation.
 * @ingroup MASSTREE LOGTYPE
 * @details
 * This log corresponds to StorageManager::create_masstree() opereation.
 * CREATE STORAGE has no in-epoch transaction order.
 * It is always processed in a separate epoch from operations for the storage.
 * Thus, we advance epoch right after creating a storage (before following operations).
 *
 * This log type is infrequently triggered, so no optimization. All methods defined in cpp.
 */
struct MasstreeCreateLogType : public log::StorageLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeCreateLogType)
  MasstreeMetadata metadata_;

  void apply_storage(Engine* engine, StorageId storage_id);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const MasstreeCreateLogType& v);
};

/**
 * Retrieve masstree layer information from the header of the page that contains the pointer.
 * We initially stored layer information in the log, but that might be incorrect when someone
 * moves the record to next layer between log creation and record locking, so we now extract it
 * in apply_record().
 */
inline uint8_t extract_page_layer(const void* in_page_pointer) {
  const Page* page = to_page(in_page_pointer);
  return page->get_header().masstree_layer_;
}

/**
 * @brief A base class for MasstreeInsertLogType/MasstreeDeleteLogType/MasstreeOverwriteLogType.
 * @ingroup MASSTREE LOGTYPE
 * @details
 * This defines a common layout for the log types so that composer/partitioner can easier
 * handle these log types. This means we waste a bit (eg delete log type doesn't need payload
 * offset/count), but we anyway have extra space if we want to have data_ 8-byte aligned.
 * data_ always starts with the key, followed by payload for insert/overwrite.
 */
struct MasstreeCommonLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeCommonLogType)
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
  KeySlice        get_first_slice() const { return normalize_be_bytes_full_aligned(aligned_data_); }
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

  /** used only for sanity check. returns if the record's and log's suffix keys are equal */
  bool equal_record_and_log_suffixes(const char* data) const {
    uint8_t layer = extract_page_layer(data);
    // Keys shorter than 8h+8 bytes are stored at layer <= h
    ASSERT_ND(layer <= key_length_ / sizeof(KeySlice));
    // both record and log keep 8-byte padded keys. so we can simply compare keys
    uint16_t skipped = (layer + 1U) * sizeof(KeySlice);
    if (key_length_ > skipped) {
      const char* key = get_key();
      uint16_t suffix_length = key_length_ - skipped;
      uint16_t suffix_length_aligned = assorted::align8(suffix_length);
      // both record and log's suffix keys are 8-byte aligned with zero-padding, so we can
      // compare multiply of 8 bytes
      return std::memcmp(data, key + skipped, suffix_length_aligned) == 0;
    }
    return true;
  }

  /**
   * Returns -1, 0, 1 when left is less than, same, larger than right in terms of key and xct_id.
   * @pre this->is_valid(), other.is_valid()
   * @pre this->get_ordinal() != 0, other.get_ordinal() != 0
   */
  inline static int compare_logs(
    const MasstreeCommonLogType* left,
    const MasstreeCommonLogType* right) ALWAYS_INLINE {
    ASSERT_ND(left->header_.storage_id_ == right->header_.storage_id_);
    if (left == right) {
      return 0;
    }
    if (LIKELY(left->key_length_ > 0 && right->key_length_ > 0)) {
      // Compare the first slice. This should be enough to differentiate most logs
      const char* left_key = left->get_key();
      const char* right_key = right->get_key();
      ASSERT_ND(is_key_aligned_and_zero_padded(left_key, left->key_length_));
      ASSERT_ND(is_key_aligned_and_zero_padded(right_key, right->key_length_));
      KeySlice left_slice = normalize_be_bytes_full_aligned(left_key);
      KeySlice right_slice = normalize_be_bytes_full_aligned(right_key);
      if (left_slice < right_slice) {
        return -1;
      } else if (left_slice > right_slice) {
        return 1;
      }

      // compare the rest with memcmp.
      uint16_t min_length = std::min(left->key_length_, right->key_length_);
      if (min_length > kSliceLen) {
        uint16_t remaining = min_length - kSliceLen;
        int key_result = std::memcmp(left_key + kSliceLen, right_key + kSliceLen, remaining);
        if (key_result != 0) {
          return key_result;
        }
      }
    }
    if (left->key_length_ < right->key_length_) {
      return -1;
    } else if (left->key_length_ < right->key_length_) {
      return 1;
    }

    // same key, now compare xct_id
    return left->header_.xct_id_.compare_epoch_and_orginal(right->header_.xct_id_);
  }
};


/**
 * @brief Log type of masstree-storage's insert operation.
 * @ingroup MASSTREE LOGTYPE
 * @details
 * This is different from hash.
 * In MasstreeBorderPage, we atomically increment the \e physical record count, set the key,
 * set slot, leaves it as deleted, then unlock the page lock. When we get to here,
 * the record already has key set and has reserved slot.
 */
struct MasstreeInsertLogType : public MasstreeCommonLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeInsertLogType)

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    const void* payload,
    uint16_t    payload_count) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeInsert;
    populate_base(type, storage_id, key, key_length, payload, 0, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    ASSERT_ND(owner_id->xct_id_.is_deleted());  // the physical record should be in 'deleted' status
    uint8_t layer = extract_page_layer(owner_id);
    uint16_t skipped = (layer + 1U) * sizeof(KeySlice);
    uint16_t key_length_aligned = get_key_length_aligned();
    ASSERT_ND(key_length_aligned >= skipped);
    // no need to set key in apply(). it's already set when the record is physically inserted
    // (or in other places if this is recovery).
    ASSERT_ND(equal_record_and_log_suffixes(data));
    if (payload_count_ > 0U) {
      // record's payload is also 8-byte aligned, so copy multiply of 8 bytes.
      // if the compiler is smart enough, it will do some optimization here.
      uint16_t suffix_length_aligned = key_length_aligned - skipped;
      void* data_payload = ASSUME_ALIGNED(data + suffix_length_aligned, 8U);
      const void* log_payload = ASSUME_ALIGNED(get_payload(), 8U);
      std::memcpy(data_payload, log_payload, assorted::align8(payload_count_));
    }
    ASSERT_ND(equal_record_and_log_suffixes(data));
    owner_id->xct_id_.set_notdeleted();
  }

  void            assert_valid() const ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_, payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeMasstreeInsert);
  }

  friend std::ostream& operator<<(std::ostream& o, const MasstreeInsertLogType& v);
};

/**
 * @brief Log type of masstree-storage's delete operation.
 * @ingroup MASSTREE LOGTYPE
 * @details
 * This one does nothing but flipping delete bit.
 */
struct MasstreeDeleteLogType : public MasstreeCommonLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeDeleteLogType)

  static uint16_t calculate_log_length(uint16_t key_length) ALWAYS_INLINE {
    return MasstreeCommonLogType::calculate_log_length(key_length, 0);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeDelete;
    populate_base(type, storage_id, key, key_length);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    ASSERT_ND(!owner_id->xct_id_.is_deleted());
    ASSERT_ND(equal_record_and_log_suffixes(data));
    owner_id->xct_id_.set_deleted();
  }

  void            assert_valid() const ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_));
    ASSERT_ND(header_.get_type() == log::kLogCodeMasstreeDelete);
  }

  friend std::ostream& operator<<(std::ostream& o, const MasstreeDeleteLogType& v);
};

/**
 * @brief Log type of masstree-storage's overwrite operation.
 * @ingroup MASSTREE LOGTYPE
 * @details
 * Same as insert log.
 */
struct MasstreeOverwriteLogType : public MasstreeCommonLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeOverwriteLogType)

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    const void* payload,
    uint16_t    payload_offset,
    uint16_t    payload_count) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeOverwrite;
    ASSERT_ND(payload_count > 0U);
    ASSERT_ND(key_length > 0U);
    populate_base(type, storage_id, key, key_length, payload, payload_offset, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    ASSERT_ND(!owner_id->xct_id_.is_deleted());

    uint8_t layer = extract_page_layer(owner_id);
    uint16_t skipped = (layer + 1U) * sizeof(KeySlice);
    uint16_t key_length_aligned = get_key_length_aligned();
    ASSERT_ND(equal_record_and_log_suffixes(data));

    ASSERT_ND(payload_count_ > 0U);
    // Unlike insert, we can't assume 8-bytes alignment because of payload_offset
    uint16_t suffix_length_aligned = key_length_aligned - skipped;
    std::memcpy(data + suffix_length_aligned + payload_offset_, get_payload(), payload_count_);
  }

  void            assert_valid() const ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_, payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeMasstreeOverwrite);
  }

  friend std::ostream& operator<<(std::ostream& o, const MasstreeOverwriteLogType& v);
};


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_LOG_TYPES_HPP_
