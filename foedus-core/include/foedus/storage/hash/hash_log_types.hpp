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
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
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
  uint8_t         bin_bits_;          // +1 => 19
  char            name_[5];           // +5 => 24

  static uint16_t calculate_log_length(uint16_t name_length) {
    return assorted::align8(19 + name_length);
  }

  void populate(StorageId storage_id, uint16_t name_length, const char* name, uint8_t bin_bits);
  void apply_storage(thread::Thread* context, Storage* storage);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const HashCreateLogType& v);
};

/**
 * @brief Log type of hash-storage's insert operation.
 * @ingroup HASH LOGTYPE
 * @details
 * This one does a bit more than memcpy because it has to install the record.
 * @todo This needs to actually modify slot. Maybe system transaction to prepare everything and
 * apply just flip delete bit?
 */
struct HashInsertLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(HashInsertLogType)
  uint16_t        key_length_;        // +2 => 18
  uint16_t        payload_count_;     // +2 => 20
  /** Is it inserted to the primary bin, not the alternative bin. */
  bool            bin1_;              // +1 => 21
  uint8_t         slot_;          // +1 => 22
  /**
   * This is auxiliary. We can calculate from the key, but 2 bytes is not that big waste,
   * and by doing this the data part is fully 8-byte aligned. Might be slightly faster.
   */
  uint16_t        hashtag_;           // +2 => 24
  char            data_[8];           // +8 => 32

  static uint16_t calculate_log_length(uint16_t key_length, uint16_t payload_count) ALWAYS_INLINE {
    // we pad to 8 bytes so that we always have a room for FillerLogType to align.
    return assorted::align8(24 + key_length + payload_count);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    bool        bin1,
    uint8_t     slot,
    uint16_t    hashtag,
    const void* payload,
    uint16_t    payload_count) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeHashInsert;
    header_.log_length_ = calculate_log_length(key_length, payload_count);
    header_.storage_id_ = storage_id;
    bin1_ = bin1;
    slot_ = slot;
    hashtag_ = hashtag;
    key_length_ = key_length;
    payload_count_ = payload_count;
    std::memcpy(data_, key, key_length);
    std::memcpy(data_ + key_length_, payload, payload_count);
  }

  void            apply_record(
    thread::Thread* context,
    Storage* storage,
    xct::XctId* owner_id,
    char* payload) ALWAYS_INLINE {
    ASSERT_ND(dynamic_cast<HashStorage*>(storage));
    reinterpret_cast<HashStorage*>(storage)->apply_insert_record(context, this, owner_id, payload);
  }

  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_, payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeHashInsert);
  }

  friend std::ostream& operator<<(std::ostream& o, const HashInsertLogType& v);
};


/**
 * @brief Second log type of hash-storage's insert operation. Is dummy log that goes with page TID.
 * @ingroup HASH LOGTYPE
 *
 */
struct HashInsertDummyLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(HashInsertDummyLogType)
  static uint16_t calculate_log_length() ALWAYS_INLINE { return 16; }
  void            populate(StorageId   storage_id) ALWAYS_INLINE {
    header_.log_type_code_ = log::kLogCodeHashInsertDummy;
    header_.log_length_ = calculate_log_length();
    header_.storage_id_ = storage_id;
  }
  void            apply_record(
    thread::Thread* context,
    Storage* storage,
    xct::XctId* owner_id,
    char* payload) ALWAYS_INLINE {
    return;
  }
  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_type_code_ == log::kLogCodeHashInsertDummy);
  }
  friend std::ostream& operator<<(std::ostream& o, const HashInsertDummyLogType& v);
};

/**
 * @brief Log type of hash-storage's delete operation.
 * @ingroup HASH LOGTYPE
 * @details
 * This one does nothing but flipping delete bit.
 * @todo Same above
 */
struct HashDeleteLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(HashDeleteLogType)
  uint16_t        key_length_;        // +2 => 18
  bool            bin1_;              // +1 => 19
  /** Auxilirary, but makes the search faster. */
  uint8_t         slot_;              // +1 => 20
  char            data_[4];           // +4 => 24

  static uint16_t calculate_log_length(uint16_t key_length) ALWAYS_INLINE {
    return assorted::align8(20 + key_length);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    bool        bin1,
    uint16_t    slot) ALWAYS_INLINE {
    ASSERT_ND(slot < kMaxEntriesPerBin);
    header_.log_type_code_ = log::kLogCodeHashDelete;
    header_.log_length_ = calculate_log_length(key_length);
    header_.storage_id_ = storage_id;
    bin1_ = bin1;
    slot_ = slot;
    key_length_ = key_length;
    std::memcpy(data_, key, key_length);
  }

  void            apply_record(
    thread::Thread* context,
    Storage* storage,
    xct::XctId* owner_id,
    char* payload) ALWAYS_INLINE {
    ASSERT_ND(dynamic_cast<HashStorage*>(storage));
    reinterpret_cast<HashStorage*>(storage)->apply_delete_record(context, this, owner_id, payload);
  }

  void            assert_valid() ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_));
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
struct HashOverwriteLogType : public log::RecordLogType {
  LOG_TYPE_NO_CONSTRUCT(HashOverwriteLogType)
  uint16_t        key_length_;        // +2 => 18
  uint16_t        payload_offset_;    // +2 => 20
  uint16_t        payload_count_;     // +2 => 22
  bool            bin1_;              // +1 => 23
  uint8_t         slot_;              // +1 => 24
  char            data_[1];           // +8 => 32

  static uint16_t calculate_log_length(uint16_t key_length, uint16_t payload_count) ALWAYS_INLINE {
    // we pad to 8 bytes so that we always have a room for FillerLogType to align.
    return assorted::align8(24 + key_length + payload_count);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    uint16_t    key_length,
    bool        bin1,
    uint16_t    slot,
    const void* payload,
    uint16_t    payload_offset,
    uint16_t    payload_count) ALWAYS_INLINE {
    ASSERT_ND(slot < kMaxEntriesPerBin);
    header_.log_type_code_ = log::kLogCodeHashOverwrite;
    header_.log_length_ = calculate_log_length(key_length, payload_count);
    header_.storage_id_ = storage_id;
    bin1_ = bin1;
    slot_ = slot;
    key_length_ = key_length;
    payload_offset_ = payload_offset;
    payload_count_ = payload_count;
    std::memcpy(data_, key, key_length);
    std::memcpy(data_ + key_length_, payload, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    Storage* storage,
    xct::XctId* /*owner_id*/,
    char* payload) ALWAYS_INLINE {
    ASSERT_ND(dynamic_cast<HashStorage*>(storage));
    std::memcpy(
      payload + key_length_ + payload_offset_,
      data_ + key_length_,
      payload_count_);
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
