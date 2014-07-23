/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_LOG_TYPES_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
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
  uint16_t        name_length_;       // +2 => 18
  char            name_[6];           // +6 => 24

  static uint16_t calculate_log_length(uint16_t name_length) {
    return assorted::align8(18 + name_length);
  }

  void populate(StorageId storage_id, uint16_t name_length, const char* name);
  void apply_storage(thread::Thread* context, Storage* storage);
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
 * @brief Log type of masstree-storage's insert operation.
 * @ingroup MASSTREE LOGTYPE
 * @details
 * This is different from hash.
 * In MasstreeBorderPage, we atomically increment the \e physical record count, set the key,
 * set slot, leaves it as deleted, then unlock the page lock. When we get to here,
 * the record already has key set and has reserved slot.
 */
struct MasstreeInsertLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeInsertLogType)
  /** This is the whole key length. When we apply, we only set suffix in the record. */
  uint16_t        key_length_;        // +2 => 18
  uint16_t        payload_count_;     // +2 => 20
  /**
   * This is used only when we apply the changes to volatile pages in commit protocol.
   * When we construct snapshot pages or when we recover, this is ignored.
   */
  uint8_t         layer_;             // +1 => 21
  char            data_[3];           // +3 => 24

  static uint16_t calculate_log_length(uint16_t key_length, uint16_t payload_count) ALWAYS_INLINE {
    return assorted::align8(21 + key_length + payload_count);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    const void* payload,
    uint16_t    payload_count,
    uint8_t     layer) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeMasstreeInsert;
    header_.log_length_ = calculate_log_length(key_length, payload_count);
    header_.storage_id_ = storage_id;
    key_length_ = key_length;
    payload_count_ = payload_count;
    layer_ = layer;
    std::memcpy(data_, key, key_length);
    std::memcpy(data_ + key_length_, payload, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    Storage* storage,
    Record* record) ALWAYS_INLINE {
    ASSERT_ND(dynamic_cast<MasstreeStorage*>(storage));
    ASSERT_ND(record->owner_id_.is_deleted());  // the physical record should be in 'deleted' status
    uint16_t skipped = calculate_skipped_key_length(key_length_, layer_);
    // no need to set key in apply(). it's already set when the record is physically inserted
    // (or in other places if this is recovery).
    ASSERT_ND(std::memcmp(record->payload_, data_ + skipped, key_length_ - skipped) == 0);
    std::memcpy(record->payload_ + key_length_ - skipped, data_ + key_length_, payload_count_);
    ASSERT_ND(std::memcmp(record->payload_, data_ + skipped, key_length_ - skipped) == 0);
    record->owner_id_.set_notdeleted();
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
struct MasstreeDeleteLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeDeleteLogType)
  uint16_t        key_length_;        // +2 => 18
  uint8_t         layer_;             // +1 => 19
  char            data_[5];           // +5 => 24

  static uint16_t calculate_log_length(uint16_t key_length) ALWAYS_INLINE {
    return assorted::align8(19 + key_length);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    uint8_t     layer) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeMasstreeDelete;
    header_.log_length_ = calculate_log_length(key_length);
    header_.storage_id_ = storage_id;
    key_length_ = key_length;
    layer_ = layer;
    std::memcpy(data_, key, key_length);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    Storage* storage,
    Record* record) ALWAYS_INLINE {
    ASSERT_ND(dynamic_cast<MasstreeStorage*>(storage));
    ASSERT_ND(!record->owner_id_.is_deleted());
    uint16_t skipped = calculate_skipped_key_length(key_length_, layer_);
    ASSERT_ND(std::memcmp(record->payload_, data_ + skipped, key_length_ - skipped) == 0);
    record->owner_id_.set_deleted();
    // TODO(Hideaki) currently this is overwritten by unlock. unlock must check this.
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
struct MasstreeOverwriteLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeOverwriteLogType)
  uint16_t        key_length_;        // +2 => 18
  uint16_t        payload_offset_;    // +2 => 20
  uint16_t        payload_count_;     // +2 => 22
  uint8_t         layer_;             // +1 => 23
  char            data_[1];           // +1 => 24

  static uint16_t calculate_log_length(uint16_t key_length, uint16_t payload_count) ALWAYS_INLINE {
    return assorted::align8(23 + key_length + payload_count);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    const void* payload,
    uint16_t    payload_offset,
    uint16_t    payload_count,
    uint8_t     layer) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeMasstreeOverwrite;
    header_.log_length_ = calculate_log_length(key_length, payload_count);
    header_.storage_id_ = storage_id;
    key_length_ = key_length;
    payload_offset_ = payload_offset;
    payload_count_ = payload_count;
    layer_ = layer;
    std::memcpy(data_, key, key_length);
    std::memcpy(data_ + key_length_, payload, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    Storage* storage,
    Record* record) ALWAYS_INLINE {
    ASSERT_ND(dynamic_cast<MasstreeStorage*>(storage));
    ASSERT_ND(!record->owner_id_.is_deleted());

    uint16_t skipped = calculate_skipped_key_length(key_length_, layer_);
    ASSERT_ND(std::memcmp(record->payload_, data_ + skipped, key_length_ - skipped) == 0);
    std::memcpy(
      record->payload_ + key_length_ - skipped + payload_offset_,
      data_ + key_length_,
      payload_count_);
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
