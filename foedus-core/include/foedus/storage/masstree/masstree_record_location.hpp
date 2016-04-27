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
#ifndef FOEDUS_STORAGE_MASSTREE_RECORD_LOCATION_HPP_
#define FOEDUS_STORAGE_MASSTREE_RECORD_LOCATION_HPP_

#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
  * @brief return value of MasstreeStoragePimpl::locate_record()/reserve_record().
  * @details
  * See foedus::storage::hash::RecordLocation regarding \e observed_.
  * Masstree's locate_record() is also now a \e logical operation.
  * @see foedus::storage::hash::RecordLocation
  */
struct RecordLocation {
  /** The border page containing the record. */
  MasstreeBorderPage* page_;
  /** Index of the record in the page. kBorderPageMaxSlots if not found. */
  SlotIndex index_;
  /**
    * TID as of locate_record() identifying the record.
    * See foedus::storage::hash::RecordLocation.
    */
  xct::XctId observed_;

  /**
    * If this method took a read-set on the returned record,
    * points to the corresponding read-set. Otherwise nullptr.
    */
  xct::ReadXctAccess* readset_;

  bool is_found() const { return index_ != kBorderPageMaxSlots; }
  void clear() {
    page_ = CXX11_NULLPTR;
    index_ = kBorderPageMaxSlots;
    observed_.data_ = 0;
    readset_ = CXX11_NULLPTR;
  }

  /**
    * Populates the result with XID and possibly readset.
    * This is a \e logical operation that might take a readset.
    */
  ErrorCode populate_logical(
    xct::Xct* cur_xct,
    MasstreeBorderPage* page,
    SlotIndex index,
    bool intended_for_write);
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_RECORD_LOCATION_HPP_
