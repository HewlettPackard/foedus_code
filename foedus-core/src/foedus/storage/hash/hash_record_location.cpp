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
#include "foedus/storage/hash/hash_record_location.hpp"

#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/xct/xct.hpp"

namespace foedus {
namespace storage {
namespace hash {

ErrorCode RecordLocation::populate_logical(
  xct::Xct* cur_xct,
  HashDataPage* page,
  DataPageSlotIndex index,
  bool intended_for_write) {
  page_ = page;
  index_ = index;
  readset_ = nullptr;

  HashDataPage::Slot* slot = page->get_slot_address(index);
  record_ = page->record_from_offset(slot->offset_);
  key_length_ = slot->key_length_;  // key_length is immutable, so just read it.
  physical_record_length_ = slot->physical_record_length_;  // immutable, too.

  // cur_payload_length_ is mutable, so must be retrieved _as of_ the XID.
  // we thus need a retry loop.
  while (true) {
    xct::XctId before = slot->tid_.xct_id_;
    assorted::memory_fence_consume();
    cur_payload_length_ = slot->payload_length_;
    assorted::memory_fence_consume();

    // [Logical check]: Read XID in a finalized fashion. If we found it "moved",
    // then we don't need to remember it as read-set.
    CHECK_ERROR_CODE(cur_xct->on_record_read(
      intended_for_write,
      &slot->tid_,
      &observed_,
      &readset_,
      true,       // no_readset_if_moved. see below
      true));     // no_readset_if_next_layer. see below

    ASSERT_ND(!observed_.is_being_written());
    ASSERT_ND(!observed_.is_next_layer());  // we don't use this flag in hash. never occurs
    ASSERT_ND(observed_.is_valid());

    if (observed_ == before) {
      // The consume fences make sure we are seeing in this order.
      // 1. read "before"
      // 2. read payload_length_
      // 3. read observed_
      // thus, observed==before guarantees that payload_length_ is *as of* observed_.
      break;
    }
  }

  if (observed_.is_moved()) {
    ASSERT_ND(readset_ == nullptr);
  }
  return kErrorCodeOk;
}

void RecordLocation::populate_physical(HashDataPage* page, DataPageSlotIndex index) {
  page_ = page;
  index_ = index;
  readset_ = nullptr;

  HashDataPage::Slot* slot = page->get_slot_address(index);
  cur_payload_length_ = slot->payload_length_;  // no worry on physical-only read.
  key_length_ = slot->key_length_;
  physical_record_length_ = slot->physical_record_length_;
  record_ = page->record_from_offset(slot->offset_);
  observed_ = slot->tid_.xct_id_.spin_while_being_written();
  ASSERT_ND(!observed_.is_being_written());
  ASSERT_ND(!observed_.is_next_layer());  // we don't use this flag in hash. never occurs
  ASSERT_ND(observed_.is_valid());
}


}  // namespace hash
}  // namespace storage
}  // namespace foedus
