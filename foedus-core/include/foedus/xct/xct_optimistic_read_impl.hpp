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
#ifndef FOEDUS_XCT_XCT_OPTIMISTIC_READ_IMPL_HPP_
#define FOEDUS_XCT_XCT_OPTIMISTIC_READ_IMPL_HPP_

#include "foedus/error_stack.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_access.hpp"

namespace foedus {
namespace xct {

/**
 * @brief implements the optimistic read protocol with version checking and appropriate fences.
 * @ingroup XCT
 * @details
 * This is a generic optimistic read logic that receives the actual reading as lambda parameter.
 * @attention handler must be idempotent because this method retries.
 * See MasstreeStoragePimpl::increment_general() for notes on common mistake.
 */
template <typename HANDLER>
inline ErrorCode optimistic_read_protocol(
  thread::Thread* context,
  Xct* xct,
  storage::StorageId storage_id,
  RwLockableXctId* owner_id_address,
  bool in_snapshot,
  HANDLER handler) {
  if (in_snapshot || xct->get_isolation_level() == kDirtyRead) {
    CHECK_ERROR_CODE(handler(owner_id_address->xct_id_));
    return kErrorCodeOk;
  }

  XctId observed(owner_id_address->xct_id_);
  assorted::memory_fence_consume();
  CHECK_ERROR_CODE(handler(observed));

  // The Masstree paper additionally has another fence and version-check and then a retry if the
  // version differs. However, in our protocol such thing is anyway caught in commit phase.
  // Thus, we have only one fence here.
  return xct->add_to_read_set(context, storage_id, observed, owner_id_address, false, false);
}

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_OPTIMISTIC_READ_IMPL_HPP_
