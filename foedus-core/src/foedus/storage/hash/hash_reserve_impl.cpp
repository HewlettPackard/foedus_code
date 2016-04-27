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
#include "foedus/storage/hash/hash_reserve_impl.hpp"

#include <glog/logging.h>

#include "foedus/assert_nd.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {

ErrorCode ReserveRecords::run(xct::SysxctWorkspace* sysxct_workspace) {
  ASSERT_ND(aggressive_payload_count_hint_ >= payload_count_);
  return find_or_create_or_expand(sysxct_workspace, target_, hint_check_from_);
}

ErrorCode ReserveRecords::find_or_create_or_expand(
  xct::SysxctWorkspace* sysxct_workspace,
  HashDataPage* page,
  DataPageSlotIndex examined_records) {
  ASSERT_ND(!page->header().snapshot_);
  // lock the page first so that there is no race on new keys.
  VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
  if (next_pointer.is_null()) {
    CHECK_ERROR_CODE(context_->sysxct_page_lock(sysxct_workspace, reinterpret_cast<Page*>(page)));
    next_pointer = page->next_page().volatile_pointer_;
  } else {
    // If the page already has a next-page pointer, the page is already finalized. Skip locking.
    assorted::memory_fence_acquire();  // must check next-page BEFORE everything else
  }
  // In either case, from now on this page is finalized
  ASSERT_ND(page->is_locked() || !next_pointer.is_null());
  const DataPageSlotIndex count = target_->get_record_count();
  ASSERT_ND(examined_records <= count);

  // Locate the key in this page.
  DataPageSlotIndex index;
  if (examined_records == count) {
#ifndef NDEBUG
    // is examined_records correct? let's confirm
    index = search_within_page(page, examined_records, 0);
    ASSERT_ND(index == kSlotNotFound);
#endif  // NDEBUG
    index = kSlotNotFound;
  } else {
    // We can skip the first examined_records records.
    // We took the page lock, so physical-only search is enough.
    index = search_within_page(page, count, examined_records);
  }

  if (index == kSlotNotFound) {
    // still no match, This is simpler.
    if (!next_pointer.is_null()) {
      // We have a next page, let's go on.
      // This is so far a recursion. Easier to debug than a loop.
      // Stackoverflow? If you have a long chain in hash bucket, you are already screwed
      HashDataPage* next_page = context_->resolve_cast<HashDataPage>(next_pointer);
      return find_or_create_or_expand(sysxct_workspace, next_page, 0);
    } else {
      // This is the tail page. and we locked it.
      // We simply create a new physical record.
      ASSERT_ND(page->is_locked());
      return create_new_record_in_tail_page(page);
    }
  } else {
    // We found a match! This is either super easy or complex
    ASSERT_ND(index >= examined_records);
    ASSERT_ND(index < count);
    if (page->get_slot(index).get_max_payload() >= payload_count_) {
      // This record is enough spacious. we are done! a super easy case.
      out_slot_ = index;
      out_page_ = page;
      return kErrorCodeOk;
    } else {
      // We need to expand this record. To do that, we need to lock this record and move it.
      // This can be complex.
      return expand_record(sysxct_workspace, page, index);
    }
  }
}

ErrorCode ReserveRecords::expand_record(
  xct::SysxctWorkspace* sysxct_workspace,
  HashDataPage* page,
  DataPageSlotIndex index) {
  auto* record = &page->get_slot(index).tid_;
  CHECK_ERROR_CODE(context_->sysxct_record_lock(
    sysxct_workspace,
    page->get_volatile_page_id(),
    record));
  // After locking, now the record status is finalized.
  if (record->is_moved()) {
    // If we now find it moved, we have two choices.
    //  1) resume the search. the new location should be somewhere after here.
    //  2) retry the whole sysxct.
    // We do 2) to save coding (yikes!). Excuse: This case should be rare.
    LOG(INFO) << "Rare. The record turns out to be moved after locking. Retry the sysxct";
    return kErrorCodeXctRaceAbort;
  }

  // Now, because we locked a record of the key, and because it wasn't moved yet,
  // we are sure that this is the only record in this hash bucket that might contain this key.
  // We'll be done as soon as we figure out where to move this record.
  HashDataPage* tail;
  CHECK_ERROR_CODE(find_and_lock_spacious_tail(sysxct_workspace, page, &tail));
  ASSERT_ND(tail->is_locked());
  ASSERT_ND(tail->available_space()
    >= HashDataPage::required_space(key_length_, aggressive_payload_count_hint_));

  // Copy the XID of the existing record
  out_slot_ = append_record_to_page(tail, record->xct_id_);
  out_page_ = tail;

  // Now we mark the old record as moved
  assorted::memory_fence_release();
  record->xct_id_.set_moved();
  return kErrorCodeOk;
}

ErrorCode ReserveRecords::find_and_lock_spacious_tail(
  xct::SysxctWorkspace* sysxct_workspace,
  HashDataPage* from_page,
  HashDataPage** tail) {
  *tail = nullptr;
  HashDataPage* cur = from_page;
  while (true) {
    VolatilePagePointer next_pointer = cur->next_page().volatile_pointer_;
    if (!next_pointer.is_null()) {
      cur = context_->resolve_cast<HashDataPage>(next_pointer);
      continue;
    }
    // cur looks like a tail page, but we need to confirm that after locking.
    CHECK_ERROR_CODE(context_->sysxct_page_lock(sysxct_workspace, reinterpret_cast<Page*>(cur)));
    if (!cur->next_page().volatile_pointer_.is_null()) {
      LOG(INFO) << "Rare. Someone has just made a next page";
      continue;
    }

    const uint16_t available_space = cur->available_space();
    const uint16_t required_space
      = HashDataPage::required_space(key_length_, aggressive_payload_count_hint_);
    if (available_space >= required_space) {
      *tail = cur;
      return kErrorCodeOk;
    } else {
      // need to make a new page..
      HashDataPage* new_tail;
      CHECK_ERROR_CODE(create_new_tail_page(cur, &new_tail));

      // Then, after all, we "publish" this new page
      assorted::memory_fence_release();  // so that others don't see uninitialized page
      cur->next_page().volatile_pointer_ = new_tail->get_volatile_page_id();
      assorted::memory_fence_release();  // so that others don't have "where's the next page" issue
      cur->header().page_version_.set_has_next_page();

      // We could immediately make a record in this page before the publish above.
      // If we do that we could avoid lock here. But the code becomes a bit uglier.
      // record-expand is anyway a costly operation, so ok for now.
      CHECK_ERROR_CODE(context_->sysxct_page_lock(
        sysxct_workspace,
        reinterpret_cast<Page*>(new_tail)));
      *tail = new_tail;
      return kErrorCodeOk;
    }
  }
}

ErrorCode ReserveRecords::create_new_tail_page(
  HashDataPage* cur_tail,
  HashDataPage** new_tail) {
  ASSERT_ND(cur_tail->is_locked());
  DVLOG(2) << "Volatile HashDataPage is full. Adding a next page..";
  const VolatilePagePointer new_pointer
    = context_->get_thread_memory()->grab_free_volatile_page_pointer();
  if (UNLIKELY(new_pointer.is_null())) {
    return kErrorCodeMemoryNoFreePages;
  }

  *new_tail = context_->resolve_newpage_cast<HashDataPage>(new_pointer.get_offset());
  (*new_tail)->initialize_volatile_page(
    cur_tail->header().storage_id_,
    new_pointer,
    reinterpret_cast<Page*>(cur_tail),
    cur_tail->get_bin(),
    cur_tail->get_bin_shifts());
  return kErrorCodeOk;
}

ErrorCode ReserveRecords::create_new_record_in_tail_page(HashDataPage* tail) {
  ASSERT_ND(tail->is_locked());
  ASSERT_ND(tail->next_page().volatile_pointer_.is_null());

  // do we have enough room in this page?
  const uint16_t available_space = tail->available_space();
  const uint16_t required_space
    = HashDataPage::required_space(key_length_, aggressive_payload_count_hint_);
  xct::XctId initial_xid;
  initial_xid.set(
    Epoch::kEpochInitialCurrent,  // TODO(Hideaki) this should be something else
    0);
  initial_xid.set_deleted();
  if (available_space < required_space) {
    // This page can't hold it. Let's make a new page.
    HashDataPage* new_tail;
    CHECK_ERROR_CODE(create_new_tail_page(tail, &new_tail));

    // Then append a record to it.
    out_slot_ = append_record_to_page(new_tail, initial_xid);
    ASSERT_ND(out_slot_ == 0);  // it's the first record in the new page
    out_page_ = new_tail;
    // Then, after all, we "publish" this new page
    assorted::memory_fence_release();  // so that others don't see uninitialized page
    tail->next_page().volatile_pointer_ = new_tail->get_volatile_page_id();
    assorted::memory_fence_release();  // so that others don't have "where's the next page" issue
    tail->header().page_version_.set_has_next_page();
  } else {
    // the page is enough spacious, and has no next page. we rule!
    out_slot_ = append_record_to_page(tail, initial_xid);
    out_page_ = tail;
  }

  return kErrorCodeOk;
}

DataPageSlotIndex ReserveRecords::search_within_page(
  const HashDataPage* page,
  DataPageSlotIndex key_count,
  DataPageSlotIndex examined_records) const {
  ASSERT_ND(!page->header().snapshot_);
  ASSERT_ND(page->header().page_version_.is_locked()
    || !page->next_page().volatile_pointer_.is_null());
  ASSERT_ND(key_count == page->get_record_count());
  return page->search_key_physical(
    combo_.hash_,
    combo_.fingerprint_,
    key_,
    key_length_,
    key_count,
    examined_records);
}

DataPageSlotIndex ReserveRecords::append_record_to_page(
  HashDataPage* page,
  xct::XctId initial_xid) const {
  ASSERT_ND(page->available_space()
    >= HashDataPage::required_space(key_length_, aggressive_payload_count_hint_));
  DataPageSlotIndex index = page->get_record_count();
  auto& slot = page->get_new_slot(index);
  slot.offset_ = page->next_offset();
  slot.hash_ = combo_.hash_;
  slot.key_length_ = key_length_;
  slot.physical_record_length_ = assorted::align8(key_length_) + assorted::align8(payload_count_);
  slot.payload_length_ = 0;
  char* record = page->record_from_offset(slot.offset_);
  std::memcpy(record, key_, key_length_);
  if (key_length_ % 8 != 0) {
    std::memset(record + key_length_, 0, 8 - (key_length_ % 8));
  }
  slot.tid_.reset();
  slot.tid_.xct_id_ = initial_xid;

  // we install the fingerprint to bloom filter BEFORE we increment key count.
  // it's okay for concurrent reads to see false positives, but false negatives are wrong!
  page->bloom_filter_.add(combo_.fingerprint_);

  // we increment key count AFTER installing the key because otherwise the optimistic read
  // might see the record but find that the key doesn't match. we need a fence to prevent it.
  assorted::memory_fence_release();
  page->header_.increment_key_count();
  return index;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
