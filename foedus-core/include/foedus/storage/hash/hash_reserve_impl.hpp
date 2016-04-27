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
#ifndef FOEDUS_STORAGE_HASH_HASH_RESERVE_IMPL_HPP_
#define FOEDUS_STORAGE_HASH_HASH_RESERVE_IMPL_HPP_

#include "foedus/error_code.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_combo.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/sysxct_functor.hpp"

namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief A system transaction to reserve a physical record(s) in a hash data page.
 * @ingroup HASH
 * @see SYSXCT
 * @details
 * A record insertion happens in two steps:
 * \li During Execution: Physically inserts a deleted record for the key in the data page.
 * \li During Commit: Logically flips the delete-bit and installs the payload.
 *
 * This system transaction does the former.
 *
 * This does nothing and returns kErrorCodeOk in the following cases:
 * \li The page turns out to already contain a satisfying physical record for the key.
 *
 * In all other cases, this sysxct creates or finds the record.
 * In other words, this sysxct guarantees a success as far as it returns kErrorCodeOk.
 *
 * Locks taken in this sysxct (in order of taking):
 * \li Page-lock of the target or the tail page if target_ is not the tail
 * (in the order of the chain, so no deadlock).
 * \li Record-lock of an existing, matching record. Only when we have to expand the record.
 * This also happens after the page-lock of the enclosing page, before the page-lock of
 * following pages, so every lock is in order.
 *
 * Note, however, that it might not be in address order (address order might be tail -> head).
 * We thus might have a false deadlock-abort. However, as far as the first lock on the
 * target_ page is unconditional, all locks after that should be non-racy, so all try-lock
 * should succeed. We might want to specify a bit larger max_retries (5?) for this reason.
 *
 * @par Differences from Masstree's reserve
 * This sysxct is simpler than masstree::ReserveRecords for a few reasons.
 * \li No need to split the page
 * \li No need to adopt a split page
 *
 * We thus contain all required logics (reserve/expansion/migrate) in this sysxct.
 * In all circumstances, this sysxct finds or reserves the required record.
 * Simpler for the caller.
 *
 * So far this sysxct installs only one physical record at a time.
 * TASK(Hideaki): Probably it helps by batching several records.
 */
struct ReserveRecords final : public xct::SysxctFunctor {
  /** Thread context */
  thread::Thread* const  context_;
  /**
   * @brief The data page to install a new physical record.
   * @details
   * This might NOT be the head page of the hash bin.
   * The contract here is that the caller must be sure that
   * any pages before this page in the chain must not contain a record
   * of the given key that is not marked as moved.
   * In other words, the caller must have checked that there is no such record
   * before hint_check_from_ of target_.
   *
   * \li Example 1: the caller found an existing record in target_ that is not moved.
   * hint_check_from_ < target_->get_key_count(), and target_ might not be the tail of the bin.
   * \li Example 2: the caller found no existing non-moved record.
   * hint_check_from_ == target_->get_key_count(), and target_ is the tail of the bin.
   *
   * Of course, by the time this sysxct takes a lock, other threads might insert
   * more records, some of which might have the key. This sysxct thus needs to
   * resume search from target_ and hint_check_from_ (but, not anywhere before!).
   */
  HashDataPage* const     target_;
  /** The key of the new record */
  const void* const       key_;
  /**
   * Hash info of the key.
   * It's &, so lifetime of the caller's HashCombo object must be longer than this sysxct.
   */
  const HashCombo&        combo_;
  /** Byte length of the key */
  const KeyLength         key_length_;
  /** Minimal required length of the payload */
  const PayloadLength     payload_count_;
  /**
   * When we expand the record or allocate a new record, we might
   * allocate a larger-than-necessary space guided by this hint.
   * It's useful to avoid future record expansion.
   * @pre aggressive_payload_count_hint_ >= payload_count_
   */
  const PayloadLength     aggressive_payload_count_hint_;
  /**
   * The in-page location from which this sysxct will look for matching records.
   * The caller is \e sure that no record before this position can match the slice.
   * Thanks to append-only writes in data pages, the caller can guarantee that the records
   * it observed are final.
   *
   * In most cases, this is same as key_count after locking, thus completely avoiding the re-check.
   * In some cases, the caller already found a matching key in this index, but even in that case
   * this sysxct must re-search after locking because the record might be now moved.
   */
  const DataPageSlotIndex hint_check_from_;

  /**
   * [Out] The slot of the record that is found or created.
   * As far as this sysxct returns kErrorCodeOk, this guarantees the following.
   * @post out_slot_ != kSlotNotFound
   * @post out_page_'s out_slot_ is at least at some point a valid non-moved record
   * of the given key with satisfying max-payload length.
   */
  DataPageSlotIndex       out_slot_;
  /**
   * [Out] The page that contains the found/created record.
   * As far as this sysxct returns kErrorCodeOk, out_page_ is either the target_ itself
   * or some page after target_.
   * @post out_page_ != nullptr
   */
  HashDataPage*           out_page_;

  ReserveRecords(
    thread::Thread* context,
    HashDataPage* target,
    const void* key,
    KeyLength key_length,
    const HashCombo& combo,
    PayloadLength payload_count,
    PayloadLength aggressive_payload_count_hint,
    DataPageSlotIndex hint_check_from)
    : xct::SysxctFunctor(),
      context_(context),
      target_(target),
      key_(key),
      combo_(combo),
      key_length_(key_length),
      payload_count_(payload_count),
      aggressive_payload_count_hint_(aggressive_payload_count_hint),
      hint_check_from_(hint_check_from),
      out_slot_(kSlotNotFound),
      out_page_(nullptr) {
  }
  virtual ErrorCode run(xct::SysxctWorkspace* sysxct_workspace) override;

  /**
   * The main loop (well, recursion actually).
   */
  ErrorCode find_or_create_or_expand(
    xct::SysxctWorkspace* sysxct_workspace,
    HashDataPage* page,
    DataPageSlotIndex examined_records);

  ErrorCode expand_record(
    xct::SysxctWorkspace* sysxct_workspace,
    HashDataPage* page,
    DataPageSlotIndex index);

  ErrorCode find_and_lock_spacious_tail(
    xct::SysxctWorkspace* sysxct_workspace,
    HashDataPage* from_page,
    HashDataPage** tail);

  /**
   * Installs it as a fresh-new physical record, assuming the given page is the tail
   * and already locked.
   */
  ErrorCode create_new_record_in_tail_page(HashDataPage* tail);

  ErrorCode create_new_tail_page(
    HashDataPage* cur_tail,
    HashDataPage** new_tail);

  DataPageSlotIndex search_within_page(
    const HashDataPage* page,
    DataPageSlotIndex key_count,
    DataPageSlotIndex examined_records) const;
  /**
   * Appends a new physical record to the page.
   * The caller must make sure there is no race in the page.
   * In other words, the page must be either locked or a not-yet-published page.
   */
  DataPageSlotIndex append_record_to_page(HashDataPage* page, xct::XctId initial_xid) const;
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_RESERVE_IMPL_HPP_
