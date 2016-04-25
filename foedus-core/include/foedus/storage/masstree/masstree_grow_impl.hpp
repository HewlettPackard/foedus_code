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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_GROW_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_GROW_IMPL_HPP_

#include "foedus/error_code.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/sysxct_functor.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief A system transaction to grow a first-layer root.
 * @ingroup MASSTREE
 * @see SYSXCT
 * @details
 * The root page of a first-layer in masstree is a bit special.
 * We thus have a separate sysxct.
 *
 * This does nothing and returns kErrorCodeOk in the following cases:
 * \li The special lock in control block \e seems contended.
 * \li The first-layer root turns out to be not moved (already grown).
 *
 * Grow is a non-mandatory operation. We can delay it.
 *
 * Locks taken in this sysxct:
 * \li A special lock that protects first-layer root pointer in control block.
 * \li Page-lock of the current first-layer root (will be retired).
 */
struct GrowFirstLayerRoot final : public xct::SysxctFunctor {
  /** Thread context */
  thread::Thread* const           context_;
  /** ID of the masstree storage to grow */
  StorageId                       storage_id_;

  GrowFirstLayerRoot(thread::Thread* context, StorageId storage_id)
    : xct::SysxctFunctor(),
      context_(context),
      storage_id_(storage_id) {
  }
  virtual ErrorCode run(xct::SysxctWorkspace* sysxct_workspace) override;
};


/**
 * @brief A system transaction to grow a second- or depper-layer root.
 * @ingroup MASSTREE
 * @see SYSXCT
 * @details
 * This does nothing and returns kErrorCodeOk in the following cases:
 * \li The root turns out to be not moved (already grown).
 * \li The parent turns out to be moved (split).
 *
 * Grow is a non-mandatory operation. We can delay it.
 *
 * Locks taken in this sysxct:
 * \li Record lock on the pointer in parent page.
 * \li Page-lock of the current root (will be retired).
 */
struct GrowNonFirstLayerRoot final : public xct::SysxctFunctor {
  /** Thread context */
  thread::Thread* const           context_;
  /**
   * The border page of the parent layer.
   */
  MasstreeBorderPage* const       parent_;
  /**
   * Index of the pointer in parent.
   */
  const uint16_t                  pointer_index_;

  GrowNonFirstLayerRoot(
    thread::Thread* context,
    MasstreeBorderPage* parent,
    uint16_t pointer_index)
    : xct::SysxctFunctor(),
      context_(context),
      parent_(parent),
      pointer_index_(pointer_index) {
  }
  virtual ErrorCode run(xct::SysxctWorkspace* sysxct_workspace) override;
};


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_GROW_IMPL_HPP_
