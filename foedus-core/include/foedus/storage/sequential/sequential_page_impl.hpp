/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_PAGE_IMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_PAGE_IMPL_HPP_
#include <stdint.h>

#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/epoch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/storage/page.hpp"
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
 * In \ref SEQUENTIAL, every data page is a leaf page that forms a singly linked list. So simple.
 *
 * This is a private implementation-details of \ref SEQUENTIAL, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class SequentialPage final {
 public:
  /**
   * Stores each record's length. We do NOT store position of each record,
   * so we have to sum up all of them to identify the location.
   * However, that's the only usecase of \ref SEQUENTIAL. Only scans.
   */
  struct LengthSlots {
    /**
     * Length of each record's payload.
     * Zero means that the record is not inserted yet (we don't allow zero-byte body
     * in \ref SEQUENTIAL; there is no point to have it).
     */
    uint16_t lengthes_[kMaxSlots];
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  SequentialPage() = delete;
  SequentialPage(const SequentialPage& other) = delete;
  SequentialPage& operator=(const SequentialPage& other) = delete;

  // simple accessors
  PageHeader&         header() { return header_; }
  const PageHeader&   header() const { return header_; }

  /**
   * Returns how many records in this page placed so far. Monotonically increasing.
   * When the page becomes full, we go on to next page.
   * @invariant 0 <= record count <= kMaxSlots
   * @note this method might return a stale information on volatile page. Use
   * barrier or atomic CAS when needed.
   */
  uint16_t            get_record_count()  const {
    return static_cast<uint16_t>(peek_status() >> 16);  // 32-48 bits
  }
  /**
   * How many data bytes in this page consumed so far. Monotonically increasing.
   * When the page becomes full, we go on to next page.
   * @invariant 0 <= used data bytes <= kDataSize
   * @note this method might return a stale information on volatile page. Use
   * barrier or atomic CAS when needed.
   */
  uint16_t            get_used_data_bytes() const {
    return static_cast<uint16_t>(peek_status());  // 48-64 bits
  }

  /** Returns byte length of payload of the specified record in this page. */
  uint16_t            get_payload_length(uint16_t record)  const {
    ASSERT_ND(record < get_record_count());
    return slots_.lengthes_[record];
  }
  /** Returns byte length of the specified record in this page. */
  uint16_t            get_record_length(uint16_t record)  const {
    return get_payload_length(record) + foedus::storage::kRecordOverhead;
  }

  /** Retrieve positions and lengthes of all records in one batch. */
  void                get_all_records_nosync(
    uint16_t* record_count,
    const char** record_pointers,
    uint16_t* payload_lengthes) const {
    *record_count = get_record_count();
    uint16_t position = 0;
    for (uint16_t i = 0; i < *record_count; ++i) {
      record_pointers[i] = data_ + position;
      payload_lengthes[i] = slots_.lengthes_[i];
      position += assorted::align8(payload_lengthes[i]) + foedus::storage::kRecordOverhead;
    }
  }

  DualPagePointer&        next_page() { return next_page_; }
  const DualPagePointer&  next_page() const { return next_page_; }

  /** Called only when this page is initialized. */
  void                initialize_data_page(StorageId storage_id, uint64_t page_id) {
    header_.checksum_ = 0;
    header_.page_id_ = page_id;
    header_.storage_id_ = storage_id;
    status_.store(0, std::memory_order_relaxed);
    next_page_.snapshot_pointer_ = 0;
    next_page_.volatile_pointer_.word = 0;
  }

  /**
   * @brief Appends a record to this page while usual transactional processing.
   * @return whether it successfully inserted. False if this page becomes full
   * due to concurrent threads. In that case, the caller will allocate a new page.
   * @details
   * This method invokes one atomic operation to safely insert a new record in a
   * lock-free fashion.
   */
  bool                append_record(
    xct::XctId owner_id,
    uint16_t payload_length,
    const void* payload);

  /**
   * @brief Appends a record to this page while snapshotting.
   * @details
   * This method assumes that there is no concurrent thread modifying this page (that's how
   * we compose pages while snapshotting) and that the page has enough space.
   */
  void                append_record_nosync(
    xct::XctId owner_id,
    uint16_t payload_length,
    const void* payload) {
    uint16_t record = get_record_count();
    ASSERT_ND(record < kMaxSlots);
    uint16_t used_data_bytes = get_used_data_bytes();
    ASSERT_ND(used_data_bytes + assorted::align8(payload_length) + kRecordOverhead <= kDataSize);
    slots_.lengthes_[record] = payload_length;
    xct::XctId* owner_id_addr = reinterpret_cast<xct::XctId*>(data_ + used_data_bytes);
    *owner_id_addr = owner_id;
    std::memcpy(data_ + used_data_bytes + kRecordOverhead, payload, payload_length);
    status_.store(
      (static_cast<uint64_t>(record + 1) << 16) |
        static_cast<uint64_t>(used_data_bytes + assorted::align8(payload_length) + kRecordOverhead),
      std::memory_order_relaxed);
  }

  /**
   * Returns if this page has enough room to insert a record with the given payload length.
   * This method does not guarantee if you can really insert it due to concurrent appends.
   * Thus, this method should be followed by append_record, which does real check and insert.
   */
  bool                can_insert_record(uint16_t payload_length) const {
    uint16_t record_length = assorted::align8(payload_length) + kRecordOverhead;
    return get_used_data_bytes() + record_length <= kDataSize && get_record_count() < kMaxSlots;
  }
  /**
   * Returns the epoch of the first record in this page (invalid epoch if no record).
   * In a volatile sequential page, we make sure one page doesn't have records in two epochs.
   * In snapshot pages, we don't have such requirements.
   */
  Epoch               get_first_record_epoch() const {
    if (get_record_count() == 0) {
      return INVALID_EPOCH;
    }
    const xct::XctId* owner_id_addr = reinterpret_cast<const xct::XctId*>(data_);
    return owner_id_addr->get_epoch();
  }

  /**
   * @brief Atomically sets the closed flag in status_.
   * @post closed flag is ON (whether by this caller or concurrent callers)
   * @return Whether the caller is the thread that set the flag. If this returns true,
   * the caller is responsible for installing next page. Others will spinlock for it.
   */
  bool                try_close_page();

  uint64_t            peek_status() const { return status_.load(std::memory_order_relaxed); }

 private:
  PageHeader            header_;          // +16 -> 16

  /**
   * @brief Atomically maintained status of this page.
   * @details
   * This contains all the in-memory synchronization information for this page.
   * Insertion and reads on in-memory pages access this with atomic operations
   * or barriers. If the page is a snapshot page, no need for synchronization.
   *
   *  \li [0-31) bits: Unused so far.
   *  \li [31-32) bits: whether this page is closed for further insertion.
   * If this bit is ON, next_page_ is already set or being set.
   *  \li [32-48) bits: record count.
   *  \li [48-64) bits: used data bytes.
   */
  std::atomic<uint64_t> status_;          // +8 ->24

  /**
   * Pointer to next page.
   * It is first null, then set with atomic CAS (losers of the race discards their
   * intermediate pages).
   * Once it is set, the pointer and the pointed page will never be changed.
   */
  DualPagePointer       next_page_;       // +16 -> 40

  /** Indicates lengthes of each record. */
  LengthSlots           slots_;           // +512 -> 552

  /** Dynamic records in this page. */
  char                  data_[kDataSize];
};
STATIC_SIZE_CHECK(sizeof(SequentialPage), 1 << 12)

/**
 * @brief Represents one stable root page in \ref SEQUENTIAL.
 * @ingroup SEQUENTIAL
 * @details
 * In \ref SEQUENTIAL, a root page is merely a set of pointers to \e head pages,
 * which are the beginnings of singly-linked list of data pages.
 * Root pages themselves can form singly-linked list if there are many head pages
 * after many snapshotting (on head page for each node in each snapshot, so
 * it can be many in a large machine).
 *
 * All contents of root pages are stable. They are never dynamically changed.
 * The volatile part of \ref SEQUENTIAL is maintained as a set of singly-linked list pointed
 * directly from the storage object, so there is no root page for it.
 *
 * This is a private implementation-details of \ref SEQUENTIAL, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class SequentialRootPage final {
 public:
  // A page object is never explicitly instantiated. You must reinterpret_cast.
  SequentialRootPage() = delete;
  SequentialRootPage(const SequentialRootPage& other) = delete;
  SequentialRootPage& operator=(const SequentialRootPage& other) = delete;

  // simple accessors
  PageHeader&         header() { return header_; }
  const PageHeader&   header() const { return header_; }

  /** Returns How many pointers to head pages exist in this page. */
  uint16_t            get_pointer_count()  const { return pointer_count_; }

  SnapshotPagePointer get_next_page() const { return next_page_; }
  void                set_next_page(SnapshotPagePointer page) { next_page_ = page; }

  /** Called only when this page is initialized. */
  void                initialize_data_page(StorageId storage_id) {
    header_.checksum_ = 0;
    header_.page_id_ = 0;
    header_.storage_id_ = storage_id;
    pointer_count_ = 0;
    next_page_ = 0;
  }

 private:
  PageHeader          header_;          // +16 -> 16

  /**
   * How many pointers to head pages exist in this page.
   * 64bit is too much, but we don't need any other info in this page. Kind of a filler, too.
   */
  uint64_t            pointer_count_;   // +8 -> 24

  /** Pointer to next root page. */
  SnapshotPagePointer next_page_;       // +8 -> 32

  /** Pointers to heads of data pages. */
  SnapshotPagePointer head_page_pointers_[kRootPageMaxHeadPointers];
};
STATIC_SIZE_CHECK(sizeof(SequentialRootPage), 1 << 12)

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_PAGE_IMPL_HPP_
