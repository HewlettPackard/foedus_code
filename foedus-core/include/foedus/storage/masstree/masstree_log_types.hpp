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

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
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
 * Calculate number of bytes we skip for suffix handling.
 * In general, (layer+1) * 8 bytes are skipped as that part is already represented by slices,
 * but often the entire key length is shorter than that. For example:
 *  \li layer=0, keylen=12 -> 8 bytes to skip
 *  \li layer=10,keylen=100 -> 88 bytes to skip
 *  \li layer=10,keylen=82 -> 82 bytes to skip
 */
inline uint16_t calculate_skipped_key_length(uint16_t key_length, uint8_t layer) {
  uint16_t skipped = (layer + 1) * sizeof(KeySlice);
  if (key_length < skipped) {
    return key_length;
  } else {
    return skipped;
  }
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
  /**
   * Note, this has nothing with snapshot page. Composer might make a different choice
   * on when to make next layer. This value is precise only on volatile world where
   * we lock the record before applying.
   */
  uint8_t         layer_;             // +1 => 23
  uint8_t         reserved_;          // +1 => 24
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
  char*           get_payload() { return aligned_data_ + assorted::align8(key_length_); }
  const char*     get_payload() const { return aligned_data_ + assorted::align8(key_length_); }
  void            populate_base(
    log::LogCode  type,
    StorageId     storage_id,
    uint8_t       layer,
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
    layer_ = layer;
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

  /**
   * Returns -1, 0, 1 when left is less than, same, larger than right in terms of key,
   * xct_id, then pointer (which means position in buffer).
   * @pre this->is_valid(), other.is_valid()
   * @pre this->get_ordinal() != 0, other.get_ordinal() != 0
   */
  static int compare_key_and_xct_id(
    const MasstreeCommonLogType* left,
    const MasstreeCommonLogType* right) {
    ASSERT_ND(left->header_.storage_id_ == right->header_.storage_id_);
    if (left == right) {
      return 0;
    }

    // so far we simply compare with memcmp. No KeySlice optimization.
    uint16_t min_length = std::min(left->key_length_, right->key_length_);
    int key_result = std::memcmp(left->get_key(), right->get_key(), min_length);
    if (key_result != 0) {
      return key_result;
    }
    if (left->key_length_ < right->key_length_) {
      return -1;
    } else if (left->key_length_ < right->key_length_) {
      return 1;
    }

    // same key, now compare xct_id
    int xct_cmp = left->header_.xct_id_.compare_epoch_and_orginal(right->header_.xct_id_);
    if (xct_cmp != 0) {
      return xct_cmp;
    }

    // if all of them are the same, this must be log entries of one transaction on same key.
    // in that case the log position tells chronological order.
    if (left < right) {
      return -1;
    } else {
      return 1;
    }
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
    uint16_t    payload_count,
    uint8_t     layer) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeInsert;
    populate_base(type, storage_id, layer, key, key_length, payload, 0, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* payload) ALWAYS_INLINE {
    ASSERT_ND(owner_id->xct_id_.is_deleted());  // the physical record should be in 'deleted' status
    uint16_t skipped = calculate_skipped_key_length(key_length_, layer_);
    // no need to set key in apply(). it's already set when the record is physically inserted
    // (or in other places if this is recovery).
    ASSERT_ND(std::memcmp(payload, get_key() + skipped, key_length_ - skipped) == 0);
    if (payload_count_ > 0U) {
      std::memcpy(payload + key_length_ - skipped, get_payload(), payload_count_);
    }
    ASSERT_ND(std::memcmp(payload, get_key() + skipped, key_length_ - skipped) == 0);
    owner_id->xct_id_.set_notdeleted();
  }

  void            assert_valid() ALWAYS_INLINE {
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
    uint16_t    key_length,
    uint8_t     layer) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeDelete;
    populate_base(type, storage_id, layer, key, key_length);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* payload) ALWAYS_INLINE {
    ASSERT_ND(!owner_id->xct_id_.is_deleted());
    uint16_t skipped = calculate_skipped_key_length(key_length_, layer_);
    ASSERT_ND(std::memcmp(payload, get_key() + skipped, key_length_ - skipped) == 0);
    owner_id->xct_id_.set_deleted();
  }

  void            assert_valid() ALWAYS_INLINE {
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
    uint16_t    payload_count,
    uint8_t     layer) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeOverwrite;
    populate_base(type, storage_id, layer, key, key_length, payload, payload_offset, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::LockableXctId* owner_id,
    char* payload) ALWAYS_INLINE {
    ASSERT_ND(!owner_id->xct_id_.is_deleted());

    uint16_t skipped = calculate_skipped_key_length(key_length_, layer_);
    ASSERT_ND(std::memcmp(payload, get_key() + skipped, key_length_ - skipped) == 0);
    std::memcpy(payload + key_length_ - skipped + payload_offset_, get_payload(), payload_count_);
  }

  void            assert_valid() ALWAYS_INLINE {
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
