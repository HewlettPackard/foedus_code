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
#include "foedus/storage/masstree/masstree_record_location.hpp"

#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/xct/xct.hpp"

namespace foedus {
namespace storage {
namespace masstree {

ErrorCode RecordLocation::populate_logical(
  xct::Xct* cur_xct,
  MasstreeBorderPage* page,
  SlotIndex index,
  bool intended_for_write) {
  page_ = page;
  index_ = index;
  readset_ = nullptr;

  // [Logical check]: Read XID in a finalized fashion. If we found it "moved",
  // then we don't need to remember it as read-set.
  CHECK_ERROR_CODE(cur_xct->on_record_read(
    intended_for_write,
    page->get_owner_id(index),
    &observed_,
    &readset_,
    true,       // no_readset_if_moved. see below
    true));     // no_readset_if_next_layer. see below
  ASSERT_ND(!observed_.is_being_written());
  ASSERT_ND(observed_.is_valid());

  if (observed_.is_moved() || observed_.is_next_layer()) {
    // Once it becomes a next-layer or moved pointer, it never goes back to a normal record.
    // so, there is no point to keep the read-set acquired in on_record_read().
    // We thus give parameters above that tell on_record_read() to not take read-set in the case.
    ASSERT_ND(readset_ == nullptr);  // let's confirm it
  }
  return kErrorCodeOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
