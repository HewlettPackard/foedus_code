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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_ADOPT_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_ADOPT_IMPL_HPP_

#include "foedus/error_code.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/sysxct_functor.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief A system transaction to adopt foster twins into an intermediate page.
 * @ingroup MASSTREE
 * @see SYSXCT
 * @details
 * This system transaction follows page-splits, which leave foster twins.
 * The sysxct replaces an existing separator and pointer
 * to the \e old-page with two new separators and pointers to the \e grandchildren.
 * The \e parent-page thus need a room for one more separator/pointer,
 * which might require page split.
 *
 * There are following cases:
 * \li \b Case-A -- Easies/fastest case. One of the grandchildren is an empty-range page.
 * We simply discard such a page, thus we just replace the old pointer in parent page with
 * a pointer to the non-empty grandchild.
 * \li \b Case-B -- Normal cases. The grandchildren are not empty.
 * When the pointer to old-page is the last entry of a non-full minipage in parent page,
 * we can append at last of minipage. The same applies to the case where the pointer is the
 * last entry of a full minipage that is the last minipage in a non-full parent page.
 * Otherwise, we have to split the parent page (see below).
 *
 * The caller of this sysxct reads and follows pointers without page-latch or fences
 * because reads/traversals must be lock-free and as efficient as possible.
 * This sysxct must make sure we are replacing the right pointer with adopted pointer(s).
 *
 * This does nothing and returns kErrorCodeOk in the following cases:
 * \li The parent page turns out to be moved.
 * \li The old page turns out to be retired (already adopted).
 *
 * Locks taken in this sysxct:
 * \li Page-lock of the parent page and old page (will be retied).
 *
 * So far this sysxct adopts only one child at a time.
 * TASK(Hideaki): Probably it helps by delaying and batching several adoptions.
 * @see SplitIntermediate
 */
struct Adopt final : public xct::SysxctFunctor {
  /** Thread context */
  thread::Thread* const           context_;
  /** The parent page that currently points to the old page */
  MasstreeIntermediatePage* const parent_;
  /**
   * The old page that was split, whose foster-twins are being adopted.
   * @pre old_->is_moved()
   */
  MasstreePage* const             old_;

  Adopt(
    thread::Thread* context,
    MasstreeIntermediatePage* parent,
    MasstreePage* old)
    : xct::SysxctFunctor(),
      context_(context),
      parent_(parent),
      old_(old) {
  }
  virtual ErrorCode run(xct::SysxctWorkspace* sysxct_workspace) override;

  ErrorCode adopt_case_a(
    uint16_t minipage_index,
    uint16_t pointer_index);

  ErrorCode adopt_case_b(
    uint16_t minipage_index,
    uint16_t pointer_index);
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_ADOPT_IMPL_HPP_
