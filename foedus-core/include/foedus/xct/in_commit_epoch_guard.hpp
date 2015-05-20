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
#ifndef FOEDUS_XCT_IN_COMMIT_EPOCH_GUARD_HPP_
#define FOEDUS_XCT_IN_COMMIT_EPOCH_GUARD_HPP_

#include "foedus/assert_nd.hpp"
#include "foedus/epoch.hpp"
#include "foedus/assorted/atomic_fences.hpp"

namespace foedus {
namespace xct {

/**
 * @brief Automatically sets \b in-commit-epoch with appropriate fence during pre-commit protocol.
 * @ingroup XCT
 * @details
 * If a transaction is currently committing with some log to publish, in-commit epoch
 * gives the \e conservative estimate (although usually exact) of the commit epoch.
 *
 * This class guards the range from a read-write transaction starts committing until it publishes
 * or discards the logs.
 *
 * This is used by the epoch chime to tell if it can assume that this transaction already got a
 * new epoch or not in commit phase. If it's not the case, the chime will spin on this until
 * this returns 0 or epoch that is enough recent. Without this mechanisim, we will get a too
 * conservative value of "min(ctid_w)" (Sec 4.10 [TU2013]) when there are some threads that
 * are either idle or spending long time before/after commit.
 *
 * The transaction takes an appropriate fence before updating this value so that
 * followings are guaranteed:
 * \li When this returns 0, this transaction will not publish any more log without getting
 * recent epoch (see destructor of InCommitEpochGuard).
 * \li If this returns epoch-X, the transaction will never publish a log whose epoch is less
 * than X. (this is assured by taking InCommitEpochGuard BEFORE the first fence in commit)
 * \li As an added guarantee, this value will be updated as soon as the commit phase ends, so
 * the chime can safely spin on this value.
 *
 * @note A similar protocol seems implemented in MIT SILO, too. See
 * how "txn_logger::advance_system_sync_epoch" updates per_thread_sync_epochs_ and
 * system_sync_epoch_. However, not quite sure about their implementation. Will ask.
 * @see foedus::xct::XctManagerPimpl::precommit_xct_readwrite()
 */
struct InCommitEpochGuard {
  InCommitEpochGuard(Epoch* in_commit_epoch_address, Epoch conservative_epoch)
    : in_commit_epoch_address_(in_commit_epoch_address) {
    ASSERT_ND(*in_commit_epoch_address_ == INVALID_EPOCH);
    *in_commit_epoch_address_ = conservative_epoch;
    // We assume the caller of this constructer puts a release or ack_rel fence right after this.
    // So, we don't bother putting the fence here.
    // assorted::memory_fence_release();
  }
  ~InCommitEpochGuard() {
    // prohibit reordering any other change BEFORE the update to in_commit_epoch_.
    // This is to satisfy the first requirement:
    // ("When this returns 0, this transaction will not publish any more log without getting
    // recent epoch").
    // Without this fence, chime can potentially miss the log that has been just published
    // with the old epoch.
    assorted::memory_fence_release();
    *in_commit_epoch_address_ = INVALID_EPOCH;
    // We can also call another memory_order_release here to immediately publish it,
    // but it's anyway rare. The spinning chime will eventually get the update, so no need.
    // In non-TSO architecture, this also saves some overhead in critical path.
  }
  Epoch* const in_commit_epoch_address_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_IN_COMMIT_EPOCH_GUARD_HPP_
