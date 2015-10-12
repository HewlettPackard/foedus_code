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
  KeyLength       key_length_;        // +2 => 18
  PayloadLength   payload_offset_;    // +2 => 20
  PayloadLength   payload_count_;     // +2 => 22
  uint16_t        reserved_;          // +2 => 24
  /**
   * Full key and (if exists) payload data, both of which are padded to 8 bytes.
   * By padding key part to 8 bytes, slicing becomes more efficient.
   */
  char            aligned_data_[8];   // ~ (+align8(key_length_)+align8(payload_count_))

  static uint16_t calculate_log_length(
    KeyLength key_length,
    PayloadLength payload_count) ALWAYS_INLINE {
    return 24U + assorted::align8(key_length) + assorted::align8(payload_count);
  }

  char*           get_key() { return aligned_data_; }
  const char*     get_key() const { return aligned_data_; }
  KeySlice        get_first_slice() const { return normalize_be_bytes_full_aligned(aligned_data_); }
  KeyLength       get_key_length_aligned() const { return assorted::align8(key_length_); }
  char*           get_payload() {
    return reinterpret_cast<char*>(ASSUME_ALIGNED(aligned_data_ + get_key_length_aligned(), 8U));
  }
  const char*     get_payload() const {
    return reinterpret_cast<const char*>(
      ASSUME_ALIGNED(aligned_data_ + get_key_length_aligned(), 8U));
  }

  void            populate_base(
    log::LogCode  type,
    StorageId     storage_id,
    const void*   key,
    KeyLength     key_length,
    const void*   payload = CXX11_NULLPTR,
    PayloadLength payload_offset = 0,
    PayloadLength payload_count = 0) ALWAYS_INLINE {
    header_.log_type_code_ = type;
    header_.log_length_ = calculate_log_length(key_length, payload_count);
    header_.storage_id_ = storage_id;
    key_length_ = key_length;
    payload_offset_ = payload_offset;
    payload_count_ = payload_count;
    reserved_ = 0;

    std::memcpy(aligned_data_, key, key_length);
    KeyLength aligned_key_length = assorted::align8(key_length);
    if (aligned_key_length != key_length) {
      std::memset(aligned_data_ + key_length, 0, aligned_key_length - key_length);
    }
    if (payload_count > 0) {
      PayloadLength aligned_payload_count = assorted::align8(payload_count);
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
    KeyLength skipped = (layer + 1U) * sizeof(KeySlice);
    if (key_length_ > skipped) {
      const char* key = get_key();
      KeyLength suffix_length = key_length_ - skipped;
      KeyLength suffix_length_aligned = assorted::align8(suffix_length);
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
      KeyLength min_length = std::min(left->key_length_, right->key_length_);
      if (min_length > kSliceLen) {
        KeyLength remainder = min_length - kSliceLen;
        int key_result = std::memcmp(left_key + kSliceLen, right_key + kSliceLen, remainder);
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

  struct RecordAddresses {
    PayloadLength* record_payload_count_;
    char* record_payload_;
  };

  /**
   * @brief Common parts of apply_record() in these log types that extract relevant
   * addresses from the record header.
   * @param[in,out] owner_id TID of the record
   * @param[out] record data-region of the record _AS OF taking the write-set_.
   * @pre This record must be locked by this thread.
   * @return RecordAddresses, to which the type-specific logic applies the change
   * @details
   * This method mostly consists of assertions and straightforward calculations of offsets.
   * The only notable thing is the handling of records \b whose \b offsets \b have \b changed since
   * we took it as write-set.
   *
   * @par When it happens
   * When the record must contain more space, expand_record() runs a system transaction that might
   * migrate the record in the same page. When it happens, it \b keeps \b the \b same \b TID because
   * the sysxct does nothing logically and the new page layout keeps the previous record intact.
   * Reading xcts are safe to read either new or old records, and don't have to abort unless
   * the TID actually changes when writing xcts update the TID.
   *
   * @par What to do then
   * Remember, both old and new record regions are complete and correct.
   * The only thing we have to be careful is that writing xcts write to the new record region.
   * If they write to the old place, we lose the change.
   * Fortunately, it is trivial for writing xcts because they always lock the records first.
   * We can safely retrieve the new offset in apply_record() because lock implies full barriers.
   * We just overwrite \e "record" pointer.
   *
   * @par Other approches considered
   * We also tried a simpler approach, in sysxct increments TID when it migrates the record in-page.
   * While it is \e "correct", we of course observed unnecessary aborts especially in transactions
   * that caused the migration themselves. So, we
   */
  inline RecordAddresses apply_record_prepare(xct::RwLockableXctId* owner_id, char* record) const {
    ASSERT_ND(owner_id->is_keylocked());
    ASSERT_ND(!owner_id->xct_id_.is_next_layer());
    ASSERT_ND(!owner_id->xct_id_.is_moved());
    const uint8_t layer = extract_page_layer(owner_id);
    const KeyLength skipped = (layer + 1U) * sizeof(KeySlice);
    const KeyLength key_length_aligned = this->get_key_length_aligned();
    ASSERT_ND(key_length_aligned >= skipped);
    const KeyLength suffix_length_aligned = key_length_aligned - skipped;
    // no need to set key in apply(). it's already set when the record is physically inserted
    // (or in other places if this is recovery).
    ASSERT_ND(equal_record_and_log_suffixes(record));

    // In the MasstreeBorderPage Slot, lengthes come right after TID.
    // [0]: offset, [1]: physical_record_length_, [3]: payload_length_, [4]: remainder_length_
    // [5]: original_physical_record_length_, [6]: original_offset_
    uint16_t* lengthes = reinterpret_cast<uint16_t*>(owner_id + 1);
    const DataOffset offset = lengthes[0];
    ASSERT_ND(reinterpret_cast<uint64_t>(reinterpret_cast<uintptr_t>(record)) % kPageSize
      == static_cast<uint64_t>(offset + kBorderPageDataPartOffset));
    ASSERT_ND(lengthes[1] >= suffix_length_aligned + assorted::align8(this->payload_count_));
    ASSERT_ND(lengthes[1] + offset + kBorderPageDataPartOffset <= kPageSize);
    ASSERT_ND(lengthes[1] >= suffix_length_aligned + assorted::align8(lengthes[3]));
    ASSERT_ND(lengthes[4] == this->key_length_ - (layer * sizeof(KeySlice)));

    Page* page = to_page(record);
    char* page_char = reinterpret_cast<char*>(page);
    ASSERT_ND(record == page_char + offset + kBorderPageDataPartOffset);
    const DataOffset old_offset = (record - page_char) - kBorderPageDataPartOffset;
    if (old_offset != offset) {
      // This happens only when we expanded the record
      ASSERT_ND(lengthes[5] > lengthes[1]);
      // Data-region grows forward, so the new offset must be larger
      ASSERT_ND(lengthes[6] < offset);
      ASSERT_ND(old_offset < offset);

      record = page_char + kBorderPageDataPartOffset + offset;
      ASSERT_ND(equal_record_and_log_suffixes(record));
    }

    PayloadLength* record_payload_count = lengthes + 3;
    // record's payload is also 8-byte aligned, so copy multiply of 8 bytes.
    // if the compiler is smart enough, it will do some optimization here.
    char* record_payload = reinterpret_cast<char*>(
      ASSUME_ALIGNED(record + suffix_length_aligned, 8U));
    RecordAddresses ret = { record_payload_count, record_payload };
    return ret;
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
    KeyLength   key_length,
    const void* payload,
    PayloadLength payload_count) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeInsert;
    populate_base(type, storage_id, key, key_length, payload, 0, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::RwLockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    RecordAddresses addresses = apply_record_prepare(owner_id, data);
    ASSERT_ND(owner_id->xct_id_.is_deleted());
    *addresses.record_payload_count_ = payload_count_;
    if (payload_count_ > 0U) {
      const char* log_payload = get_payload();
      std::memcpy(addresses.record_payload_, log_payload, assorted::align8(payload_count_));
    }
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

  static uint16_t calculate_log_length(KeyLength key_length) ALWAYS_INLINE {
    return MasstreeCommonLogType::calculate_log_length(key_length, 0);
  }

  void            populate(
    StorageId   storage_id,
    const void* key,
    KeyLength   key_length) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeDelete;
    populate_base(type, storage_id, key, key_length);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::RwLockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    apply_record_prepare(owner_id, data);  // In this log type, just for sanity checks
    ASSERT_ND(!owner_id->xct_id_.is_deleted());
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
 * @brief Log type of masstree-storage's update operation.
 * @ingroup MASSTREE LOGTYPE
 * @details
 * Almost same as insert except this keeps the delete-flag, which must be off.
 */
struct MasstreeUpdateLogType : public MasstreeCommonLogType {
  LOG_TYPE_NO_CONSTRUCT(MasstreeUpdateLogType)

  void            populate(
    StorageId   storage_id,
    const void* key,
    KeyLength   key_length,
    const void* payload,
    PayloadLength payload_count) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeUpdate;
    populate_base(type, storage_id, key, key_length, payload, 0, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::RwLockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    RecordAddresses addresses = apply_record_prepare(owner_id, data);
    ASSERT_ND(!owner_id->xct_id_.is_deleted());
    *addresses.record_payload_count_ = payload_count_;
    if (payload_count_ > 0U) {
      const char* log_payload = get_payload();
      std::memcpy(addresses.record_payload_, log_payload, assorted::align8(payload_count_));
    }
  }

  void            assert_valid() const ALWAYS_INLINE {
    assert_valid_generic();
    ASSERT_ND(header_.log_length_ == calculate_log_length(key_length_, payload_count_));
    ASSERT_ND(header_.get_type() == log::kLogCodeMasstreeUpdate);
  }

  friend std::ostream& operator<<(std::ostream& o, const MasstreeUpdateLogType& v);
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
    KeyLength   key_length,
    const void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count) ALWAYS_INLINE {
    log::LogCode type = log::kLogCodeMasstreeOverwrite;
    ASSERT_ND(payload_count > 0U);
    ASSERT_ND(key_length > 0U);
    populate_base(type, storage_id, key, key_length, payload, payload_offset, payload_count);
  }

  void            apply_record(
    thread::Thread* /*context*/,
    StorageId /*storage_id*/,
    xct::RwLockableXctId* owner_id,
    char* data) const ALWAYS_INLINE {
    RecordAddresses addresses = apply_record_prepare(owner_id, data);
    ASSERT_ND(!owner_id->xct_id_.is_deleted());
    ASSERT_ND(*addresses.record_payload_count_ >= payload_count_ + payload_offset_);
    if (payload_count_ > 0U) {
      const char* log_payload = get_payload();
      std::memcpy(
        addresses.record_payload_ + payload_offset_,
        log_payload,
        payload_count_);
    }
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
