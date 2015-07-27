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
 * @par Page Layout
 * See @ref SEQ_LAYOUT
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
  uint16_t            get_record_count()  const { return record_count_; }
  /**
   * How many data bytes in this page consumed so far. Monotonically increasing.
   * When the page becomes full, we go on to next page.
   * @invariant 0 <= used data bytes <= kDataSize
   * @note this method might return a stale information on volatile page. Use
   * barrier or atomic CAS when needed.
   */
  uint16_t            get_used_data_bytes() const { return used_data_bytes_; }

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
  /** Returns beginning offset of the specified record */
  uint16_t            get_record_offset(uint16_t record) const {
    assert_consistent();
    ASSERT_ND(record < get_record_count());
    uint16_t offset = 0;
    for (uint16_t i = 0; i < record; ++i) {
      uint16_t payload_length = get_payload_length(i);
      offset += assorted::align8(payload_length) + foedus::storage::kRecordOverhead;
    }
    return offset;
  }

  DualPagePointer&        next_page() { return next_page_; }
  const DualPagePointer&  next_page() const { return next_page_; }

  /** Called only when this page is initialized. */
  void                initialize_volatile_page(StorageId storage_id, VolatilePagePointer page_id) {
    header_.init_volatile(page_id, storage_id, kSequentialPageType);
    record_count_ = 0;
    used_data_bytes_ = 0;
    next_page_.snapshot_pointer_ = 0;
    next_page_.volatile_pointer_.word = 0;
  }
  void                initialize_snapshot_page(StorageId storage_id, SnapshotPagePointer page_id) {
    header_.init_snapshot(page_id, storage_id, kSequentialPageType);
    record_count_ = 0;
    used_data_bytes_ = 0;
    next_page_.snapshot_pointer_ = 0;
    next_page_.volatile_pointer_.word = 0;
  }

  /**
   * @brief Appends a record to this page.
   * @details
   * This method assumes that there is no concurrent thread modifying this page (that's how
   * we write both volatile and snapshot pages) and that the page has enough space.
   */
  void                append_record_nosync(
    xct::XctId owner_id,
    uint16_t payload_length,
    const void* payload) {
    uint16_t record = get_record_count();
    ASSERT_ND(record < kMaxSlots);
    ASSERT_ND(used_data_bytes_ + assorted::align8(payload_length) + kRecordOverhead <= kDataSize);
    set_payload_length(record, payload_length);
    xct::LockableXctId* owner_id_addr = owner_id_from_offset(used_data_bytes_);
    owner_id_addr->xct_id_ = owner_id;
    owner_id_addr->lock_.reset();  // not used...
    std::memcpy(data_ + used_data_bytes_ + kRecordOverhead, payload, payload_length);
    ++record_count_;
    used_data_bytes_ += assorted::align8(payload_length) + kRecordOverhead;
    header_.key_count_ = record_count_;
    if (!header_.snapshot_) {
      // to protect concurrent cursor, version counter must be written at last
      assorted::memory_fence_release();
    }
    // no need for lock to increment, just a barrier suffices. directly call status_.increment.
    header_.page_version_.status_.increment_version_counter();
    assert_consistent();
  }

  /**
   * Returns if this page has enough room to insert a record with the given payload length.
   * This method does not guarantee if you can really insert it due to concurrent appends.
   * Thus, this method should be followed by append_record, which does real check and insert.
   */
  bool                can_insert_record(uint16_t payload_length) const {
    uint16_t record_length = assorted::align8(payload_length) + kRecordOverhead;
    return used_data_bytes_ + record_length
      + sizeof(PayloadLength) * (record_count_ + 1) <= kDataSize && record_count_ < kMaxSlots;
  }
  /**
   * Returns the epoch of the fist record in this page (undefined behavior if no record).
   * @pre get_record_count()
   */
  Epoch               get_first_record_epoch() const {
    ASSERT_ND(get_record_count() > 0);
    const xct::LockableXctId* first_owner_id = owner_id_from_offset(0);
    return first_owner_id->xct_id_.get_epoch();
  }

  xct::LockableXctId* owner_id_from_offset(uint16_t offset) {
    return reinterpret_cast<xct::LockableXctId*>(data_ + offset);
  }
  const xct::LockableXctId* owner_id_from_offset(uint16_t offset) const {
    return reinterpret_cast<const xct::LockableXctId*>(data_ + offset);
  }

  uint32_t              unused_dummy_func_filler() const { return filler_; }

 private:
  /** Byte length of payload is represented in 2 bytes. */
  typedef uint16_t PayloadLength;

  PageHeader            header_;            // +32 -> 32

  uint16_t              record_count_;      // +2 -> 34
  uint16_t              used_data_bytes_;   // +2 -> 36
  uint32_t              filler_;            // +4 -> 40

  /**
   * Pointer to next page.
   * Once it is set, the pointer and the pointed page will never be changed.
   */
  DualPagePointer       next_page_;         // +16 -> 56

  /**
   * Dynamic data part in this page, which consist of 1) record part growing forward,
   * 2) unused part, and 3) payload lengthes part growing backward.
   */
  char                  data_[kDataSize];
};

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
  const HeadPagePointer* get_pointers() const { return head_page_pointers_; }

  void set_pointers(HeadPagePointer *pointers, uint16_t pointer_count) {
    ASSERT_ND(pointer_count <= kRootPageMaxHeadPointers);
    pointer_count_ = pointer_count;
    std::memcpy(head_page_pointers_, pointers, sizeof(HeadPagePointer) * pointer_count);
  }

  SnapshotPagePointer get_next_page() const { return next_page_; }
  void                set_next_page(SnapshotPagePointer page) { next_page_ = page; }

  /** Called only when this page is initialized. */
  void                initialize_snapshot_page(StorageId storage_id, SnapshotPagePointer page_id) {
    header_.init_snapshot(page_id, storage_id, kSequentialRootPageType);
    pointer_count_ = 0;
    next_page_ = 0;
  }

  const char*         unused_dummy_func_filler() const { return filler_; }

 private:
  PageHeader          header_;          // +32 -> 32

  /**
   * How many pointers to head pages exist in this page.
   * 64bit is too much, but we don't need any other info in this page. Kind of a filler, too.
   */
  uint64_t            pointer_count_;   // +8 -> 40

  /** Pointer to next root page. */
  SnapshotPagePointer next_page_;       // +8 -> 48

  /** Pointers to heads of data pages. */
  HeadPagePointer     head_page_pointers_[kRootPageMaxHeadPointers];

  char                filler_[kPageSize - kRootPageHeaderSize - sizeof(head_page_pointers_)];
};

STATIC_SIZE_CHECK(sizeof(SequentialPage), 1 << 12)
STATIC_SIZE_CHECK(sizeof(SequentialRootPage), 1 << 12)

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_PAGE_IMPL_HPP_
