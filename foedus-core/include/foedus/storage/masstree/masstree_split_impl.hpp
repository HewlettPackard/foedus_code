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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_SPLIT_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_SPLIT_IMPL_HPP_

#include "foedus/error_code.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/sysxct_functor.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief A system transaction to split a border page in Master-Tree.
 * @ingroup MASSTREE
 * @see SYSXCT
 * @details
 * When a border page becomes full or close to full, we split the page into two border pages.
 * The new pages are placed as tentative foster twins of the page.
 *
 * This does nothing and returns kErrorCodeOk in the following cases:
 * \li The page turns out to be already split.
 *
 * Locks taken in this sysxct (in order of taking):
 * \li Page-lock of the target page.
 * \li Record-lock of all records in the target page (in canonical order).
 *
 * At least after releasing enclosing user transaction's locks, there is no chance of deadlocks
 * or even any conditional locks. max_retries=2 should be enough in run_nested_sysxct().
 */
struct SplitBorder final : public xct::SysxctFunctor {
  /** Thread context */
  thread::Thread* const       context_;
  /**
   * The page to split.
   * @pre !header_.snapshot_ (split happens to only volatile pages)
   */
  MasstreeBorderPage* const   target_;
  /** The key that triggered this split. A hint for NRS */
  const KeySlice              trigger_;
  /**
   * If true, we never do no-record-split (NRS).
   * This is useful for example when we want to make room for record-expansion.
   * Otherwise, we get stuck when the record-expansion causes a page-split that is eligible for NRS.
   */
  const bool                  disable_no_record_split_;
  /**
   * An optimization to also make room for a record.
   * Whether to do that, key length, payload length, and the key (well, suffix).
   * In this case, trigger_ is implicitly the slice for piggyback_reserve_.
   *
   * This optimization is best-effort. The caller must check afterwards
   * whether the space is actually reserved. For example, a concurrent thread
   * might have newly reserved a (might be too small) space  for the key right before
   * the call.
   */
  const bool                  piggyback_reserve_;
  const KeyLength             piggyback_remainder_length_;
  const PayloadLength         piggyback_payload_count_;
  const void*                 piggyback_suffix_;

  SplitBorder(
    thread::Thread* context,
    MasstreeBorderPage* target,
    KeySlice trigger,
    bool disable_no_record_split = false,
    bool            piggyback_reserve = false,
    KeyLength       piggyback_remainder_length = 0,
    PayloadLength   piggyback_payload_count = 0,
    const void*     piggyback_suffix = nullptr)
    : xct::SysxctFunctor(),
      context_(context),
      target_(target),
      trigger_(trigger),
      disable_no_record_split_(disable_no_record_split),
      piggyback_reserve_(piggyback_reserve),
      piggyback_remainder_length_(piggyback_remainder_length),
      piggyback_payload_count_(piggyback_payload_count),
      piggyback_suffix_(piggyback_suffix) {
  }
  virtual ErrorCode run(xct::SysxctWorkspace* sysxct_workspace) override;

  struct SplitStrategy {
    /**
    * whether this page seems to have had sequential insertions, in which case we do
    * "no-record split" as optimization. This also requires the trigerring insertion key
    * is equal or larger than the largest slice in this page.
    */
    bool no_record_split_;
    SlotIndex original_key_count_;
    KeySlice smallest_slice_;
    KeySlice largest_slice_;
    /**
    * This will be the new foster fence.
    * Ideally, # of records below and above this are same.
    */
    KeySlice mid_slice_;
  };

  /**
   * @brief Subroutine to decide how we will split this page.
   */
  void decide_strategy(SplitStrategy* out) const;

  /** Subroutine to lock existing records in target_ */
  ErrorCode lock_existing_records(xct::SysxctWorkspace* sysxct_workspace);

  /**
   * @brief Subroutine to construct a new page.
   */
  void migrate_records(
    KeySlice inclusive_from,
    KeySlice inclusive_to,
    MasstreeBorderPage* dest) const;
};


/**
 * @brief A system transaction to split an intermediate page in Master-Tree.
 * @ingroup MASSTREE
 * @see SYSXCT
 * @details
 * Same as border page split.
 *
 * This does nothing and returns kErrorCodeOk in the following cases:
 * \li The page turns out to be already split.
 * \li piggyback_adopt_child_ turns out to be already retired (meaning already adopted)
 *
 * Locks taken in this sysxct (in order of taking):
 * \li Page-lock of the target page, and piggyback_adopt_child_
 * if piggyback_adopt_child_ is non-null. We lock target/piggyback_adopt_child_
 * in canonical order.
 *
 * Obviously no chance of deadlocks or even any conditional locks
 * after releasing enclosing user transaction's locks.
 * max_retries=2 should be enough in run_nested_sysxct().
 */
struct SplitIntermediate final : public xct::SysxctFunctor {
  /** Thread context */
  thread::Thread* const       context_;
  /**
   * The page to split.
   * @pre !target_->header_.snapshot_ (split happens to only volatile pages)
   */
  MasstreeIntermediatePage* const target_;

  /**
   * @brief An optimization for the common case: splitting the parent page to adopt
   * foster twins of a child page.
   * @details
   * The child page whose foster-twins will be adopted in the course of this split.
   * Without this optimization, we need two Sysxct invocation, split and adopt.
   * This also gives a hint whether it might be no-record-split (NRS).
   * null if no such piggy-back adoption (in that case ignores the possibility of NRS).
   * @pre piggyback_adopt_child_ == nullptr || piggyback_adopt_child_->has_foster_child()
   */
  MasstreePage* const         piggyback_adopt_child_;

  SplitIntermediate(
    thread::Thread* context,
    MasstreeIntermediatePage* target,
    MasstreePage* piggyback_adopt_child = nullptr)
    : xct::SysxctFunctor(),
      context_(context),
      target_(target),
      piggyback_adopt_child_(piggyback_adopt_child) {
  }

  virtual ErrorCode run(xct::SysxctWorkspace* sysxct_workspace) override;

  /**
   * The core implementation after locking relevant pages and acquiring free page resource.
   * This method never fails because all lock/resource are already taken.
   * @pre target_->is_locked()
   * @pre free_pages->get_count() >= 3
   * @pre piggyback_adopt_child_ == nullptr || piggyback_adopt_child_->is_locked()
   * @pre target_->has_foster_child()) : should be checked after locking before calling
   * @pre piggyback_adopt_child_ == nullptr || !piggyback_adopt_child_->is_retired(): same above
   */
  void split_impl_no_error(
    thread::GrabFreeVolatilePagesScope* free_pages);

  /**
   * Constructed by hierarchically reading all separators and pointers in old page.
   */
  struct SplitStrategy {
    enum Constants {
      kMaxSeparators = 170,  // must be larger than kMaxIntermediatePointers
    };
    /**
    * pointers_[n] points to page that is responsible for keys
    * separators_[n - 1] <= key < separators_[n].
    * separators_[-1] is infimum.
    */
    KeySlice separators_[kMaxSeparators];  // ->1360
    DualPagePointer pointers_[kMaxSeparators];  // -> 4080
    KeySlice mid_separator_;  // -> 4088
    uint16_t total_separator_count_;  // -> 4090
    uint16_t mid_index_;  // -> 4092

    /**
     * When this says true, we create a dummy foster with empty
     * range on right side, and just re-structures target page onto left foster twin.
     * In other words, this is compaction/restructure that is done in another page in RCU fashion.
     */
    bool      compact_adopt_;   // -> 4093
    char      dummy_[3];        // -> 4096
  };

  /**
   * @brief Subroutine to decide how we will split this page.
   */
  void decide_strategy(SplitStrategy* out) const;

  /**
   * @brief Subroutine to construct a new page.
   */
  void migrate_pointers(
    const SplitStrategy& strategy,
    uint16_t from,
    uint16_t to,
    KeySlice expected_last_separator,
    MasstreeIntermediatePage* dest) const;
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_SPLIT_IMPL_HPP_
