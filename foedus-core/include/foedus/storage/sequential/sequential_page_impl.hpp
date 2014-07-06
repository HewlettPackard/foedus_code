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
    return (peek_status() >> 16) & 0x7FFFU;  // 33-48 bits
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

  void                assert_consistent() const {
    ASSERT_ND(get_used_data_bytes() + get_record_count() * sizeof(PayloadLength) <= kDataSize);
  }

  /** Returns byte length of payload of the specified record in this page. */
  uint16_t            get_payload_length(uint16_t record)  const {
    ASSERT_ND(record < get_record_count());
    assert_consistent();
    const PayloadLength* lengthes = reinterpret_cast<const PayloadLength*>(data_ + kDataSize);
    PayloadLength length = *(lengthes - 1 - record);
    return length;
  }
  /** Sets byte length of payload of the specified record in this page. */
  void                set_payload_length(uint16_t record, uint16_t length) {
    assert_consistent();
    PayloadLength* lengthes = reinterpret_cast<PayloadLength*>(data_ + kDataSize);
    *(lengthes - 1 - record) = length;
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
    assert_consistent();
    *record_count = get_record_count();
    uint16_t position = 0;
    for (uint16_t i = 0; i < *record_count; ++i) {
      record_pointers[i] = data_ + position;
      payload_lengthes[i] = get_payload_length(i);
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
    set_payload_length(record, payload_length);
    xct::XctId* owner_id_addr = reinterpret_cast<xct::XctId*>(data_ + used_data_bytes);
    *owner_id_addr = owner_id;
    std::memcpy(data_ + used_data_bytes + kRecordOverhead, payload, payload_length);
    status_.store(
      (static_cast<uint64_t>(record + 1) << 16) |
        static_cast<uint64_t>(used_data_bytes + assorted::align8(payload_length) + kRecordOverhead),
      std::memory_order_relaxed);
    assert_consistent();
  }

  /**
   * Returns if this page has enough room to insert a record with the given payload length.
   * This method does not guarantee if you can really insert it due to concurrent appends.
   * Thus, this method should be followed by append_record, which does real check and insert.
   */
  bool                can_insert_record(uint16_t payload_length) const {
    uint16_t record_count = get_record_count();
    uint16_t record_length = assorted::align8(payload_length) + kRecordOverhead;
    uint16_t used_data_bytes = get_used_data_bytes();
    return used_data_bytes + record_length + sizeof(PayloadLength) * (record_count + 1) <= kDataSize
      && record_count < kMaxSlots;
  }
  /**
   * Returns the earliest epoch of records in this page (invalid epoch if no record).
   * In a volatile sequential page, we utilize this information to efficiently discard pages
   * after snapshotting. In snapshot pages, we don't need it thus it's not maintained.
   */
  Epoch               get_earliest_record_epoch() const {
    if (get_record_count() == 0) {
      return INVALID_EPOCH;
    }
    return Epoch(static_cast<uint32_t>(peek_status() >> 32));  // 0-32 bits
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
  /** Byte length of payload is represented in 2 bytes. */
  typedef uint16_t PayloadLength;

  PageHeader            header_;          // +16 -> 16

  /**
   * @brief Atomically maintained status of this page.
   * @details
   * This contains all the in-memory synchronization information for this page.
   * Insertion and reads on in-memory pages access this with atomic operations
   * or barriers. If the page is a snapshot page, no need for synchronization.
   *
   *  \li [0-32) bits: Earliest epoch of records in this page. This is used to efficiently
   * discard volatile pages after snapshotting. We thus don't maintain this for snapshot pages.
   *  \li [32-33) bits: whether this page is closed for further insertion.
   * If this bit is ON, next_page_ is already set or being set.
   *  \li [33-48) bits: record count (thus up to 32k records per page. surely enough).
   *  \li [48-64) bits: used data bytes (not including length part, only the forward-growing part).
   */
  std::atomic<uint64_t> status_;          // +8 ->24

  /**
   * Pointer to next page.
   * The thread that changed the status_ to 'closed' is responsible to install next page.
   * Other threads do spin lock on this.
   * Once it is set, the pointer and the pointed page will never be changed.
   */
  DualPagePointer       next_page_;       // +16 -> 40

  /**
   * Dynamic data part in this page, which consist of 1) record part growing forward,
   * 2) unused part, and 3) payload lengthes part growing backward.
   */
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
  const SnapshotPagePointer* get_pointers() const { return head_page_pointers_; }

  void set_pointers(SnapshotPagePointer *pointers, uint16_t pointer_count) {
    ASSERT_ND(pointer_count <= kRootPageMaxHeadPointers);
    pointer_count_ = pointer_count;
    std::memcpy(head_page_pointers_, pointers, sizeof(SnapshotPagePointer) * pointer_count);
  }

  SnapshotPagePointer get_next_page() const { return next_page_; }
  void                set_next_page(SnapshotPagePointer page) { next_page_ = page; }

  /** Called only when this page is initialized. */
  void                initialize_root_page(StorageId storage_id, uint64_t page_id) {
    header_.checksum_ = 0;
    header_.page_id_ = page_id;
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
