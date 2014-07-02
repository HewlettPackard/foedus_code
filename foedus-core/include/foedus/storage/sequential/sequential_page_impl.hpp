/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_PAGE_IMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_PAGE_IMPL_HPP_
#include <stdint.h>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace sequential {

/**
 * @brief Represents one data page in \ref SEQUENTIAL.
 * @ingroup SEQUENTIAL
 * @details
 * In \ref SEQUENTIAL, every page is a leaf page that forms a singly linked list. So simple.
 *
 * This is a private implementation-details of \ref SEQUENTIAL, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class SequentialPage final {
 public:
  enum Constants {
    /**
     * In terms of length slots,
     */
    kMaxSlots = 1 << 8,
    /** Byte size of header in each page of sequential storage. */
    kHeaderSize = 288,
    /** Byte size of data region in each page of sequential storage. */
    kDataSize = foedus::storage::kPageSize - kHeaderSize,
    /** Payload must be shorter than this length. */
    kMaxPayload = 2048,
  };
  /**
   * Stores each record's length. We do NOT store position of each record,
   * so we have to sum up all of them to identify the location.
   * However, that's the only usecase of \ref SEQUENTIAL. Only scans.
   */
  struct LengthSlots {
    /**
     * Length of each record's payload divided by 8.
     * Thus, one record can have at most 256*8=2048 bytes. Should be a reasonable limit.
     * Zero means that the record is not inserted yet (we don't allow zero-byte body
     * in \ref SEQUENTIAL; there is no point to have it).
     */
    uint8_t lengthes_[kMaxSlots];
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  SequentialPage() = delete;
  SequentialPage(const SequentialPage& other) = delete;
  SequentialPage& operator=(const SequentialPage& other) = delete;

  // simple accessors
  StorageId           get_storage_id()    const   { return storage_id_; }
  uint16_t            get_record_count()  const   { return record_count_; }
  uint16_t            get_used_data_bytes()  const   { return used_data_bytes_; }

  /** Returns byte length of payload of the specified record in this page. */
  uint16_t            get_payload_length(uint16_t record)  const {
    ASSERT_ND(record < record_count_);
    return slots_.lengthes_[record] << 3;
  }
  /** Returns byte length of the specified record in this page. */
  uint16_t            get_record_length(uint16_t record)  const {
    return get_payload_length(record) + foedus::storage::kRecordOverhead;
  }

  Checksum            get_checksum()      const   { return checksum_; }
  void                set_checksum(Checksum checksum)     { checksum_ = checksum; }

  const DualPagePointer& get_next_page()  const   { return next_page_; }
  void                set_next_page(const DualPagePointer& page) { next_page_ = page; }

  /** Called only when this page is initialized. */
  void                initialize_data_page(StorageId storage_id) {
    storage_id_ = storage_id;
    record_count_ = 0;
    used_data_bytes_ = 0;
  }

 private:
  /** ID of the array storage. */
  StorageId           storage_id_;      // +4 -> 4

  /**
   * How many records in this page placed so far. Monotonically increasing.
   * When this is incremented, we use atomic fetch_add.
   */
  uint16_t            record_count_;    // +2 -> 6

  /**
   * How many data bytes in this page consumed so far. Monotonically increasing.
   * When we increase this value, we do atomic CAS, not atomic fetch_add, because
   * we have to make sure the following invariant.
   * When the page becomes full, we go on to next page.
   * @invariant 0 <= used_data_bytes_ <= kDataSize
   */
  uint16_t            used_data_bytes_;   // +2 -> 8

  /**
   * Pointer to next page.
   * It is first null, then set with atomic CAS (losers of the race discards their
   * intermediate pages).
   * Once it is set, the pointer will never be changed.
   */
  DualPagePointer     next_page_;       // +16 -> 24

  /**
   * Checksum of the content of this page to detect corrupted pages.
   * \b Changes only when we save it to media. No synchronization needed to access.
   */
  Checksum            checksum_;        // +8 -> 32

  /** Indicates lengthes of each record. */
  LengthSlots         slots_;           // +256 -> 288

  /** Dynamic records in this page. */
  char                data_[kDataSize];
};
STATIC_SIZE_CHECK(sizeof(SequentialPage), 1 << 12)

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_PAGE_IMPL_HPP_
