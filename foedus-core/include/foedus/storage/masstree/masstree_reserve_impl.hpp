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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_RESERVE_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_RESERVE_IMPL_HPP_

#include "foedus/error_code.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/sysxct_functor.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief A system transaction to reserve a physical record(s) in a border page.
 * @ingroup MASSTREE
 * @see SYSXCT
 * @details
 * A record insertion happens in two steps:
 * \li During Execution: Physically inserts a deleted record for the key in the border page.
 * \li During Commit: Logically flips the delete-bit and installs the payload.
 *
 * This system transaction does the former.
 *
 * This does nothing and returns kErrorCodeOk in the following cases:
 * \li The page turns out to already contain a satisfying physical record for the key.
 * \li The page turns out to be already moved.
 * \li The page turns out to need page-split to accomodate the record.
 *
 * When the 2nd or 3rd case (or 1st case with too-short payload space) happens,
 * the caller will do something else (e.g. split/follow-foster) and retry.
 *
 * Locks taken in this sysxct (in order of taking):
 * \li Page-lock of the target page.
 * \li Record-lock of an existing, matching record. Only when we have to expand the record.
 *
 * So far this sysxct installs only one physical record at a time.
 * TASK(Hideaki): Probably it helps by batching several records.
 */
struct ReserveRecords final : public xct::SysxctFunctor {
  /** Thread context */
  thread::Thread* const       context_;
  /** The page to install a new physical record */
  MasstreeBorderPage* const   target_;
  /** The slice of the key */
  const KeySlice              slice_;
  /** Suffix of the key */
  const void* const           suffix_;
  /** Length of the remainder */
  const KeyLength             remainder_length_;
  /** Minimal required length of the payload */
  const PayloadLength         payload_count_;
  /**
   * The in-page location from which this sysxct will look for matching records.
   * The caller is \e sure that no record before this position can match the slice.
   * Thanks to append-only writes in border pages, the caller can guarantee that the records
   * it observed are final.
   *
   * In most cases, this is same as key_count after locking, thus completely avoiding the re-check.
   */
  const SlotIndex             hint_check_from_;

  /**
   * When we \e CAN create a next layer for the new record, whether to make it a next-layer
   * from the beginning, or make it as a usual record first.
   * @see MasstreeMetadata::should_aggresively_create_next_layer()
   */
  const bool                  should_aggresively_create_next_layer_;
  /**
   * [Out]
   */
  bool                        out_split_needed_;

  ReserveRecords(
    thread::Thread* context,
    MasstreeBorderPage* target,
    KeySlice slice,
    KeyLength remainder_length,
    const void* suffix,
    PayloadLength payload_count,
    bool should_aggresively_create_next_layer,
    SlotIndex hint_check_from)
    : xct::SysxctFunctor(),
      context_(context),
      target_(target),
      slice_(slice),
      suffix_(suffix),
      remainder_length_(remainder_length),
      payload_count_(payload_count),
      hint_check_from_(hint_check_from),
      should_aggresively_create_next_layer_(should_aggresively_create_next_layer),
      out_split_needed_(false) {
  }
  virtual ErrorCode run(xct::SysxctWorkspace* sysxct_workspace) override;
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_RESERVE_IMPL_HPP_
