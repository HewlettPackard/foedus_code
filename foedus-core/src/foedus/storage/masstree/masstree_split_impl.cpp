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
#include "foedus/storage/masstree/masstree_split_impl.hpp"

#include <glog/logging.h>

#include <algorithm>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/rdtsc_watch.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/////////////////////////////////////////////////////////////////////////////////////
///
///                      Border node's Split
///
/////////////////////////////////////////////////////////////////////////////////////
ErrorCode SplitBorder::run(xct::SysxctWorkspace* sysxct_workspace) {
  ASSERT_ND(!target_->header().snapshot_);
  ASSERT_ND(!target_->is_empty_range());

  debugging::RdtscWatch watch;
  DVLOG(1) << "Splitting a page... ";

  // First, lock the page, The page's lock state is before all the records in the page,
  // so we can simply lock it first.
  CHECK_ERROR_CODE(context_->sysxct_page_lock(sysxct_workspace, reinterpret_cast<Page*>(target_)));

  // The lock involves atomic operation, so now all we see are finalized.
  if (target_->has_foster_child()) {
    DVLOG(0) << "Interesting. the page has been already split";
    return kErrorCodeOk;
  }
  if (target_->get_key_count() <= 1U) {
    DVLOG(0) << "This page has too few records. Can't split it";
    return kErrorCodeOk;
  }

  const SlotIndex key_count = target_->get_key_count();

  // 2 free volatile pages needed.
  // foster-minor/major (will be placed in successful case)
  memory::PagePoolOffset offsets[2];
  thread::GrabFreeVolatilePagesScope free_pages_scope(context_, offsets);
  CHECK_ERROR_CODE(free_pages_scope.grab(2));
  const auto& resolver = context_->get_local_volatile_page_resolver();

  SplitStrategy strategy;  // small. just place it on stack
  decide_strategy(&strategy);
  ASSERT_ND(target_->get_low_fence() <= strategy.mid_slice_);
  ASSERT_ND(strategy.mid_slice_ <= target_->get_high_fence());
  MasstreeBorderPage* twin[2];
  VolatilePagePointer new_page_ids[2];
  for (int i = 0; i < 2; ++i) {
    twin[i] = reinterpret_cast<MasstreeBorderPage*>(resolver.resolve_offset_newpage(offsets[i]));
    new_page_ids[i].set(context_->get_numa_node(), offsets[i]);
    twin[i]->initialize_volatile_page(
      target_->header().storage_id_,
      new_page_ids[i],
      target_->get_layer(),
      i == 0 ? target_->get_low_fence() : strategy.mid_slice_,  // low-fence
      i == 0 ? strategy.mid_slice_ : target_->get_high_fence());  // high-fence
  }

  // lock all records
  CHECK_ERROR_CODE(lock_existing_records(sysxct_workspace));

  if (strategy.no_record_split_) {
    ASSERT_ND(!disable_no_record_split_);
    // in this case, we can move all records in one memcpy.
    // well, actually two : one for slices and another for data.
    std::memcpy(twin[0]->slices_, target_->slices_, sizeof(KeySlice) * key_count);
    std::memcpy(twin[0]->data_, target_->data_, sizeof(target_->data_));
    twin[0]->set_key_count(key_count);
    twin[1]->set_key_count(0);
    twin[0]->consecutive_inserts_ = target_->is_consecutive_inserts();
    twin[1]->consecutive_inserts_ = true;
    twin[0]->next_offset_ = target_->get_next_offset();
    twin[1]->next_offset_ = 0;
    for (SlotIndex i = 0; i < key_count; ++i) {
      xct::RwLockableXctId* owner_id = twin[0]->get_owner_id(i);
      ASSERT_ND(owner_id->is_keylocked());
      owner_id->get_key_lock()->reset();  // no race
    }
  } else {
    migrate_records(
      strategy.smallest_slice_,
      strategy.mid_slice_ - 1,  // to make it inclusive
      twin[0]);
    migrate_records(
      strategy.mid_slice_,
      strategy.largest_slice_,  // this is inclusive (to avoid supremum hassles)
      twin[1]);
  }

  // Now we will install the new pages. **From now on no error-return allowed**
  assorted::memory_fence_release();
  // We install pointers to the pages AFTER we initialize the pages.
  target_->install_foster_twin(new_page_ids[0], new_page_ids[1], strategy.mid_slice_);
  free_pages_scope.dispatch(0);
  free_pages_scope.dispatch(1);
  assorted::memory_fence_release();

  // invoking set_moved is the point we announce all of these changes. take fence to make it right
  target_->get_version_address()->set_moved();
  assorted::memory_fence_release();

  // set the "moved" bit so that concurrent transactions
  // check foster-twin for read-set/write-set checks.
  for (SlotIndex i = 0; i < key_count; ++i) {
    xct::RwLockableXctId* owner_id = target_->get_owner_id(i);
    owner_id->xct_id_.set_moved();
  }

  assorted::memory_fence_release();

  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to split a page. original page physical"
    << " record count: " << static_cast<int>(key_count)
    << "->" << static_cast<int>(twin[0]->get_key_count())
    << " + " << static_cast<int>(twin[1]->get_key_count());
  return kErrorCodeOk;
}

void SplitBorder::decide_strategy(SplitBorder::SplitStrategy* out) const {
  ASSERT_ND(target_->is_locked());
  const SlotIndex key_count = target_->get_key_count();
  ASSERT_ND(key_count > 0);
  out->original_key_count_ = key_count;
  out->no_record_split_ = false;
  out->smallest_slice_ = target_->get_slice(0);
  out->largest_slice_ = target_->get_slice(0);

  // if consecutive_inserts_, we are already sure about the key distributions, so easy.
  if (target_->is_consecutive_inserts()) {
    out->largest_slice_ = target_->get_slice(key_count - 1);
    if (!disable_no_record_split_ && trigger_ > out->largest_slice_) {
      out->no_record_split_ = true;
      DVLOG(1) << "Obviously no record split. key_count=" << static_cast<int>(key_count);
      out->mid_slice_ = out->largest_slice_ + 1;
    } else {
      if (disable_no_record_split_ && trigger_ > out->largest_slice_) {
        DVLOG(1) << "No-record split was possible, but disable_no_record_split specified."
          << " simply splitting in half...";
      }
      DVLOG(1) << "Breaks a sequential page. key_count=" << static_cast<int>(key_count);
      out->mid_slice_ = target_->get_slice(key_count / 2);
    }
    return;
  }

  for (SlotIndex i = 1; i < key_count; ++i) {
    const KeySlice this_slice = target_->get_slice(i);
    out->smallest_slice_ = std::min<KeySlice>(this_slice, out->smallest_slice_);
    out->largest_slice_ = std::max<KeySlice>(this_slice, out->largest_slice_);
  }

  ASSERT_ND(key_count >= 2U);  // because it's not consecutive, there must be at least 2 records.

  {
    // even if not, there is another easy case where two "tides" mix in this page;
    // one tide from left sequentially inserts keys while another tide from right also sequentially
    // inserts keys that are larger than left tide. This usually happens at the boundary of
    // two largely independent partitions (eg multiple threads inserting keys of their partition).
    // In that case, we should cleanly separate the two tides by picking the smallest key from
    // right-tide as the separator.
    KeySlice tides_max[2];
    KeySlice second_tide_min = kInfimumSlice;
    bool first_tide_broken = false;
    bool both_tides_broken = false;
    tides_max[0] = target_->get_slice(0);
    // for example, consider the following case:
    //   1 2 32 33 3 4 34 x
    // There are two tides 1- and 32-. We detect them as follows.
    // We initially consider 1,2,32,33 as the first tide because they are sequential.
    // Then, "3" breaks the first tide. We then consider 1- and 32- as the two tides.
    // If x breaks the tide again, we give up.
    for (SlotIndex i = 1; i < key_count; ++i) {
      // look for "tide breaker" that is smaller than the max of the tide.
      // as soon as we found two of them (meaning 3 tides or more), we give up.
      KeySlice slice = target_->get_slice(i);
      if (!first_tide_broken)  {
        if (slice >= tides_max[0]) {
          tides_max[0] = slice;
          continue;  // ok!
        } else {
          // let's find where a second tide starts.
          first_tide_broken = true;
          SlotIndex first_breaker;
          for (first_breaker = 0; first_breaker < i; ++first_breaker) {
            const KeySlice breaker_slice = target_->get_slice(first_breaker);
            if (breaker_slice > slice) {
              break;
            }
          }
          ASSERT_ND(first_breaker < i);
          tides_max[0] = slice;
          ASSERT_ND(second_tide_min == kInfimumSlice);
          second_tide_min = target_->get_slice(first_breaker);
          tides_max[1] = target_->get_slice(i - 1);
          ASSERT_ND(tides_max[0] < tides_max[1]);
          ASSERT_ND(tides_max[0] < second_tide_min);
          ASSERT_ND(second_tide_min <= tides_max[1]);
        }
      } else {
        if (slice < second_tide_min && slice >= tides_max[0]) {
          tides_max[0] = slice;
          continue;  // fine, in the first tide
        } else if (slice >= tides_max[1]) {
          tides_max[1] = slice;  // okay, in the second tide
        } else {
          DVLOG(2) << "Oops, third tide. not the easy case";
          both_tides_broken = true;
          break;
        }
      }
    }

    // Already sorted? (seems consecutive_inserts_ has some false positives)
    if (!first_tide_broken) {
      if (!disable_no_record_split_ && trigger_ > out->largest_slice_) {
        out->no_record_split_ = true;
        DVLOG(1) << "Obviously no record split. key_count=" << static_cast<int>(key_count);
        out->mid_slice_ = out->largest_slice_ + 1;
      } else {
        if (disable_no_record_split_ && trigger_ > out->largest_slice_) {
          DVLOG(1) << "No-record split was possible, but disable_no_record_split specified."
            << " simply splitting in half...";
        }
        DVLOG(1) << "Breaks a sequential page. key_count=" << static_cast<int>(key_count);
        out->mid_slice_ = target_->get_slice(key_count / 2);
      }
      return;
    }

    ASSERT_ND(first_tide_broken);
    if (!both_tides_broken) {
      DVLOG(0) << "Yay, figured out two-tides meeting in a page.";
      out->mid_slice_ = second_tide_min;
      return;
    }
  }


  // now we have to pick separator. as we don't sort in-page, this is approximate median selection.
  // there are a few smart algorithm out there, but we don't need that much accuracy.
  // just randomly pick a few. good enough.
  assorted::UniformRandom uniform_random(12345);
  const SlotIndex kSamples = 7;
  KeySlice choices[kSamples];
  for (uint8_t i = 0; i < kSamples; ++i) {
    choices[i] = target_->get_slice(uniform_random.uniform_within(0, key_count - 1));
  }
  std::sort(choices, choices + kSamples);
  out->mid_slice_ = choices[kSamples / 2];

  // scan through again to make sure the new separator is not used multiple times as key.
  // this is required for the invariant "same slices must be in same page"
  while (true) {
    bool observed = false;
    bool retry = false;
    for (SlotIndex i = 0; i < key_count; ++i) {
      const KeySlice this_slice = target_->get_slice(i);
      if (this_slice == out->mid_slice_) {
        if (observed) {
          // the key appeared twice! let's try another slice.
          ++out->mid_slice_;
          retry = true;
          break;
        } else {
          observed = true;
        }
      }
    }
    if (retry) {
      continue;
    } else {
      break;
    }
  }
}

ErrorCode SplitBorder::lock_existing_records(xct::SysxctWorkspace* sysxct_workspace) {
  debugging::RdtscWatch watch;  // check how expensive this is
  ASSERT_ND(target_->is_locked());
  const SlotIndex key_count = target_->get_key_count();
  ASSERT_ND(key_count > 0);

  // We use the batched interface. It internally sorts, but has better performance if
  // we provide an already-sorted input. Remember that slots grow backwards,
  // so larger indexes have smaller lock IDs.
  xct::RwLockableXctId* record_locks[kBorderPageMaxSlots];
  for (SlotIndex i = 0; i < key_count; ++i) {
    record_locks[i] = target_->get_owner_id(key_count - 1U - i);  // larger indexes first
  }

  VolatilePagePointer page_id(target_->header().page_id_);
  CHECK_ERROR_CODE(context_->sysxct_batch_record_locks(
    sysxct_workspace,
    page_id,
    key_count,
    record_locks));

#ifndef NDEBUG
  for (SlotIndex i = 0; i < key_count; ++i) {
    xct::RwLockableXctId* owner_id = target_->get_owner_id(i);
    ASSERT_ND(owner_id->is_keylocked());
  }
#endif  // NDEBUG
  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to lock all of "
    << static_cast<int>(key_count) << " records while splitting";
  if (watch.elapsed() > (1ULL << 26)) {
    // if we see this often, we have to optimize this somehow.
    LOG(WARNING) << "wait, wait, it costed " << watch.elapsed() << " cycles to lock all of "
      << static_cast<int>(key_count) << " records while splitting!! that's a lot! storage="
      << target_->header().storage_id_
      << ", thread ID=" << context_->get_thread_id();
  }

  return kErrorCodeOk;
}

void SplitBorder::migrate_records(
  KeySlice inclusive_from,
  KeySlice inclusive_to,
  MasstreeBorderPage* dest) const {
  ASSERT_ND(target_->is_locked());
  const auto& copy_from = *target_;
  const SlotIndex key_count = target_->get_key_count();
  ASSERT_ND(dest->get_key_count() == 0);
  dest->next_offset_ = 0;
  SlotIndex migrated_count = 0;
  DataOffset unused_space = sizeof(dest->data_);
  bool sofar_consecutive = true;
  KeySlice prev_slice = kSupremumSlice;
  KeyLength prev_remainder = kMaxKeyLength;

  // Simply iterate over and memcpy one-by-one.
  // We previously did a bit more complex thing to copy as many records as
  // possible in one memcpy, but not worth it with the new page layout.
  // We will keep an eye on the cost of this method, and optimize when it becomes bottleneck.
  for (SlotIndex i = 0; i < key_count; ++i) {
    const KeySlice from_slice = copy_from.get_slice(i);
    if (from_slice >= inclusive_from && from_slice <= inclusive_to) {
      // move this record.
      auto* to_slot = dest->get_new_slot(migrated_count);
      const auto* from_slot = copy_from.get_slot(i);
      ASSERT_ND(from_slot->tid_.is_keylocked());
      const KeyLength from_remainder = from_slot->remainder_length_;
      const KeyLength from_suffix = calculate_suffix_length(from_remainder);
      const PayloadLength payload = from_slot->lengthes_.components.payload_length_;
      const KeyLength to_remainder
        = to_slot->tid_.xct_id_.is_next_layer() ? kInitiallyNextLayer : from_remainder;
      const KeyLength to_suffix = calculate_suffix_length(to_remainder);
      if (to_remainder != from_remainder) {
        ASSERT_ND(to_remainder == kInitiallyNextLayer);
        ASSERT_ND(from_remainder != kInitiallyNextLayer && from_remainder <= kMaxKeyLength);
        DVLOG(2) << "the old record is now a next-layer record, this new record can be initially"
          " a next-layer, saving space for suffixes. from_remainder=" << from_remainder;
      }

      dest->set_slice(migrated_count, from_slice);
      to_slot->tid_.xct_id_ = from_slot->tid_.xct_id_;
      to_slot->tid_.lock_.reset();
      to_slot->remainder_length_ = to_remainder;
      to_slot->lengthes_.components.payload_length_ = payload;
      // offset/physical_length set later

      if (sofar_consecutive && migrated_count > 0) {
        if (prev_slice > from_slice
          || (prev_slice == from_slice && prev_remainder > from_remainder)) {
          sofar_consecutive = false;
        }
      }
      prev_slice = from_slice;
      prev_remainder = to_remainder;

      // we migh shrink the physical record size.
      const DataOffset record_length = MasstreeBorderPage::to_record_length(to_remainder, payload);
      ASSERT_ND(record_length % 8 == 0);
      ASSERT_ND(record_length <= from_slot->lengthes_.components.physical_record_length_);
      to_slot->lengthes_.components.physical_record_length_ = record_length;
      to_slot->lengthes_.components.offset_ = dest->next_offset_;
      to_slot->original_physical_record_length_ = record_length;
      to_slot->original_offset_ = dest->next_offset_;
      dest->next_offset_ += record_length;
      unused_space -= record_length - sizeof(*to_slot);

      // Copy the record. We want to do it in one memcpy if possible.
      // Be careful on the case where suffix length has changed (kInitiallyNextLayer case)
      if (record_length > 0) {
        char* to_record = dest->get_record_from_offset(to_slot->lengthes_.components.offset_);
        if (from_suffix != to_suffix) {
          ASSERT_ND(to_remainder == kInitiallyNextLayer);
          ASSERT_ND(from_remainder != kInitiallyNextLayer && from_remainder <= kMaxKeyLength);
          ASSERT_ND(to_suffix == 0);
          // Skip suffix part and copy only the payload.
          std::memcpy(
            to_record,
            copy_from.get_record_payload(i),
            assorted::align8(payload));
        } else {
          // Copy suffix (if exists) and payload together.
          std::memcpy(to_record, copy_from.get_record(i), record_length);
        }
      }

      ++migrated_count;
      dest->set_key_count(migrated_count);
    }
  }

  dest->consecutive_inserts_ = sofar_consecutive;
}

/////////////////////////////////////////////////////////////////////////////////////
///
///                      Interior node's Split
///
/////////////////////////////////////////////////////////////////////////////////////
ErrorCode SplitIntermediate::run(xct::SysxctWorkspace* sysxct_workspace) {
  DVLOG(1) << "Preparing to split an intermediate page... ";

  // First, lock the page(s)
  Page* target_page = reinterpret_cast<Page*>(target_);
  if (piggyback_adopt_child_) {
    Page* pages[2];
    pages[0] = target_page;
    pages[1] = reinterpret_cast<Page*>(piggyback_adopt_child_);
    // lock target_ and piggyback_adopt_child_ in batched mode to avoid deadlock.
    CHECK_ERROR_CODE(context_->sysxct_batch_page_locks(sysxct_workspace, 2, pages));
  } else {
    CHECK_ERROR_CODE(context_->sysxct_page_lock(sysxct_workspace, target_page));
  }

  // The lock involves atomic operation, so now all we see are finalized.
  if (target_->has_foster_child()) {
    DVLOG(0) << "Interesting. the page has been already split";
    return kErrorCodeOk;
  }
  if (piggyback_adopt_child_ && piggyback_adopt_child_->is_retired()) {
    VLOG(0) << "Interesting. this child is now retired, so someone else has already adopted.";
    return kErrorCodeOk;  // fine. the goal is already achieved
  }

  // 3 free volatile pages needed.
  // foster-minor/major (will be placed in successful case), and SplitStrategy (will be discarded)
  memory::PagePoolOffset offsets[3];
  thread::GrabFreeVolatilePagesScope free_pages_scope(context_, offsets);
  CHECK_ERROR_CODE(free_pages_scope.grab(3));
  split_impl_no_error(&free_pages_scope);
  return kErrorCodeOk;
}
void SplitIntermediate::split_impl_no_error(thread::GrabFreeVolatilePagesScope* free_pages) {
  // similar to border page's split, but simpler in a few places because
  // 1) intermediate page doesn't have owner_id for each pointer (no lock concerns).
  // 2) intermediate page is already completely sorted.
  // thus, this is just a physical operation without any transactional behavior.
  // even not a system transaction
  ASSERT_ND(!target_->header().snapshot_);
  ASSERT_ND(!target_->is_empty_range());

  debugging::RdtscWatch watch;
  DVLOG(1) << "Splitting an intermediate page... ";

  const uint8_t key_count = target_->get_key_count();
  target_->verify_separators();

  // 3 free volatile pages needed.
  // foster-minor/major (will be placed in successful case), and SplitStrategy (will be discarded)
  const auto& resolver = context_->get_local_volatile_page_resolver();

  SplitStrategy* strategy
    = reinterpret_cast<SplitStrategy*>(resolver.resolve_offset_newpage(free_pages->get(2)));
  decide_strategy(strategy);
  const KeySlice new_foster_fence = strategy->mid_separator_;

  MasstreeIntermediatePage* twin[2];
  VolatilePagePointer new_pointers[2];
  for (int i = 0; i < 2; ++i) {
    twin[i]
      = reinterpret_cast<MasstreeIntermediatePage*>(
          resolver.resolve_offset_newpage(free_pages->get(i)));
    new_pointers[i].set(context_->get_numa_node(), free_pages->get(i));

    twin[i]->initialize_volatile_page(
      target_->header().storage_id_,
      new_pointers[i],
      target_->get_layer(),
      target_->get_btree_level(),  // foster child has the same level as foster-parent
      i == 0 ? target_->low_fence_ : new_foster_fence,
      i == 0 ? new_foster_fence : target_->high_fence_);
  }

  migrate_pointers(*strategy, 0, strategy->mid_index_ + 1, new_foster_fence, twin[0]);
  if (strategy->compact_adopt_) {
    twin[1]->set_key_count(0);  // right is empty-range
  } else {
    migrate_pointers(
      *strategy,
      strategy->mid_index_ + 1,
      strategy->total_separator_count_,
      target_->high_fence_,
      twin[1]);
  }

  // Now we will install the new pages. **From now on no error-return allowed**
  assorted::memory_fence_release();
  // We install pointers to the pages AFTER we initialize the pages.
  target_->install_foster_twin(new_pointers[0], new_pointers[1], new_foster_fence);
  free_pages->dispatch(0);
  free_pages->dispatch(1);
  assorted::memory_fence_release();
  // invoking set_moved is the point we announce all of these changes. take fence to make it right
  target_->set_moved();

  if (piggyback_adopt_child_) {
    // piggyback_adopt_child_ is adopted and retired.
    piggyback_adopt_child_->set_retired();
    context_->collect_retired_volatile_page(piggyback_adopt_child_->get_volatile_page_id());
  }

  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to split a node. original node"
    << " key count: " << static_cast<int>(key_count);

  target_->verify_separators();
}

void SplitIntermediate::decide_strategy(SplitIntermediate::SplitStrategy* out) const {
  ASSERT_ND(target_->is_locked());
  out->compact_adopt_ = false;
  out->total_separator_count_ = 0;

  // While collecting pointers from the old page, we look for the piggyback_adopt_child_
  // and replace it with new pointers (so, this increases the total # of separators).
  KeySlice old_separator = 0;
  KeySlice new_separator = 0;
  VolatilePagePointer old_pointer;
  if (piggyback_adopt_child_) {
    ASSERT_ND(piggyback_adopt_child_->is_moved());
    old_separator = piggyback_adopt_child_->get_low_fence();
    new_separator = piggyback_adopt_child_->get_foster_fence();
    old_pointer = VolatilePagePointer(piggyback_adopt_child_->header().page_id_);
  }

  bool found_old = false;
  for (MasstreeIntermediatePointerIterator iter(target_); iter.is_valid(); iter.next()) {
    const KeySlice low = iter.get_low_key();
    const KeySlice high = iter.get_high_key();
    const DualPagePointer& pointer = iter.get_pointer();
    if (piggyback_adopt_child_ && low == old_separator) {
      // Found the existing pointer to replace with foster-minor
      ASSERT_ND(pointer.volatile_pointer_.is_equivalent(old_pointer));
      ASSERT_ND(high == piggyback_adopt_child_->get_high_fence());
      out->separators_[out->total_separator_count_] = new_separator;
      out->pointers_[out->total_separator_count_].volatile_pointer_
        = piggyback_adopt_child_->get_foster_minor();
      out->pointers_[out->total_separator_count_].snapshot_pointer_ = 0;
      ++(out->total_separator_count_);

      // Also add foster-major as a new entry
      out->separators_[out->total_separator_count_] = high;
      out->pointers_[out->total_separator_count_].volatile_pointer_
        = piggyback_adopt_child_->get_foster_major();
      out->pointers_[out->total_separator_count_].snapshot_pointer_ = 0;
      ++(out->total_separator_count_);
      found_old = true;
    } else {
      out->separators_[out->total_separator_count_] = high;
      out->pointers_[out->total_separator_count_] = pointer;
      ++(out->total_separator_count_);
    }
    ASSERT_ND(piggyback_adopt_child_ == nullptr || low < old_separator || found_old);
  }

  ASSERT_ND(piggyback_adopt_child_ == nullptr || found_old);
  ASSERT_ND(out->total_separator_count_ >= 2U);
  ASSERT_ND(out->total_separator_count_ <= kMaxIntermediatePointers + 1U);

  if (out->total_separator_count_ <= kMaxIntermediatePointers) {
    // Then, compact_adopt:
    // we create a dummy foster with empty
    // range on right side, and just re-structures target page onto left foster twin.
    // In other words, this is compaction/restructure that is done in another page in RCU fashion.
    out->compact_adopt_ = true;
    out->mid_index_ = out->total_separator_count_ - 1U;
    out->mid_separator_ = target_->get_high_fence();
  } else if (piggyback_adopt_child_
    && piggyback_adopt_child_->get_foster_major()
        == out->pointers_[out->total_separator_count_ - 1].volatile_pointer_) {
    DVLOG(0) << "Seems like a sequential insert. let's do no-record split";
    out->mid_index_ = out->total_separator_count_ - 2U;
    out->mid_separator_ = out->separators_[out->mid_index_];
  } else {
    // Usual: otherwise, we split them into 50-50 distribution.
    out->mid_index_ = (out->total_separator_count_ - 1U) / 2;
    out->mid_separator_ = out->separators_[out->mid_index_];
  }
}

void SplitIntermediate::migrate_pointers(
  const SplitIntermediate::SplitStrategy& strategy,
  uint16_t from,
  uint16_t to,
  KeySlice expected_last_separator,
  MasstreeIntermediatePage* dest) const {
  // construct dest page. copy the separators and pointers.
  // we distribute them as much as possible in first level. if mini pages have little
  // entries to start with, following adoption would be only local.
  float entries_per_mini = static_cast<float>(to - from) / (kMaxIntermediateSeparators + 1);
  ASSERT_ND(to > from);
  const uint16_t move_count = to - from;

  // it looks a bit complicated because each separator is "one-off" due to first-level separator.
  // so we buffer one separator.
  float next_mini_threshold = entries_per_mini;
  uint8_t cur_mini = 0;
  uint8_t cur_mini_separators = 0;
  auto* cur_mini_page = &dest->get_minipage(0);
  cur_mini_page->pointers_[0] = strategy.pointers_[from];
  ASSERT_ND(!strategy.pointers_[from].is_both_null());
  KeySlice next_separator = strategy.separators_[from];

  for (uint16_t i = 1; i < move_count; ++i) {
    uint16_t original_index = i + from;
    ASSERT_ND(!strategy.pointers_[original_index].is_both_null());
    if (i >= next_mini_threshold && cur_mini < kMaxIntermediateSeparators) {
      // switch to next mini page. so, the separator goes to the first level
      assorted::memory_fence_release();  // set key count after all
      cur_mini_page->key_count_ = cur_mini_separators;  // close the current
      ASSERT_ND(cur_mini_page->key_count_ <= kMaxIntermediateMiniSeparators);

      dest->separators_[cur_mini] = next_separator;

      next_mini_threshold += entries_per_mini;
      cur_mini_separators = 0;
      ++cur_mini;
      cur_mini_page = &dest->get_minipage(cur_mini);
      cur_mini_page->pointers_[0] = strategy.pointers_[original_index];
    } else {
      // still the same mini page. so, the separator goes to the second level
      cur_mini_page->separators_[cur_mini_separators] = next_separator;
      ++cur_mini_separators;
      ASSERT_ND(cur_mini_separators <= kMaxIntermediateMiniSeparators);

      cur_mini_page->pointers_[cur_mini_separators] = strategy.pointers_[original_index];
    }
    next_separator = strategy.separators_[original_index];
  }
  cur_mini_page->key_count_ = cur_mini_separators;  // close the last one
  ASSERT_ND(cur_mini_page->key_count_ <= kMaxIntermediateMiniSeparators);
  assorted::memory_fence_release();
  dest->header_.set_key_count(cur_mini);  // set key count after all
  ASSERT_ND(dest->get_key_count() <= kMaxIntermediateSeparators);

  // the last separator is ignored because it's foster-fence/high-fence.
  ASSERT_ND(next_separator == expected_last_separator);

  dest->verify_separators();
}
static_assert(SplitIntermediate::SplitStrategy::kMaxSeparators > kMaxIntermediatePointers, "WTF");
STATIC_SIZE_CHECK(sizeof(SplitIntermediate::SplitStrategy), kPageSize)

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
