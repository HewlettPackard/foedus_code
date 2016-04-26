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
#include "foedus/storage/masstree/masstree_reserve_impl.hpp"

#include <glog/logging.h>

#include "foedus/assert_nd.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

inline xct::XctId get_initial_xid() {
  xct::XctId initial_id;
  initial_id.set(
    Epoch::kEpochInitialCurrent,  // TODO(Hideaki) this should be something else
    0);
  return initial_id;
}

inline MasstreeBorderPage* allocate_new_border_page(thread::Thread* context) {
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return nullptr;
  }

  const auto &resolver = context->get_local_volatile_page_resolver();
  MasstreeBorderPage* new_page
    = reinterpret_cast<MasstreeBorderPage*>(resolver.resolve_offset_newpage(offset));
  VolatilePagePointer new_page_pointer;
  new_page_pointer.set(context->get_numa_node(), offset);
  new_page->header().page_id_ = new_page_pointer.word;
  return new_page;
}

ErrorCode ReserveRecords::run(xct::SysxctWorkspace* sysxct_workspace) {
  out_split_needed_ = false;
  ASSERT_ND(!should_aggresively_create_next_layer_ || remainder_length_ > sizeof(KeySlice));
  ASSERT_ND(!target_->header().snapshot_);
  CHECK_ERROR_CODE(context_->sysxct_page_lock(sysxct_workspace, reinterpret_cast<Page*>(target_)));
  ASSERT_ND(target_->is_locked());

  // After page-lock, key-count and record-keys (not payloads) are fixed. Check them now!
  if (target_->is_moved()) {
    DVLOG(0) << "Interesting. this page has been split";
    return kErrorCodeOk;
  }
  ASSERT_ND(!target_->is_retired());
  const SlotIndex key_count = target_->get_key_count();
  const VolatilePagePointer page_id(target_->header().page_id_);
  MasstreeBorderPage::FindKeyForReserveResult match = target_->find_key_for_reserve(
    hint_check_from_,
    key_count,
    slice_,
    suffix_,
    remainder_length_);

  // Let's check whether we need to split this page.
  // If we can accomodate it without splitting the page, complete it within here.
  if (match.match_type_ == MasstreeBorderPage::kExactMatchLayerPointer) {
    // Definitelly done. The caller must folllow the next-layer
    ASSERT_ND(match.index_ < kBorderPageMaxSlots);
    return kErrorCodeOk;
  } else if (match.match_type_ == MasstreeBorderPage::kExactMatchLocalRecord) {
    // Is it enough spacious?
    ASSERT_ND(match.index_ < kBorderPageMaxSlots);
    if (target_->get_max_payload_length(match.index_) >= payload_count_) {
      return kErrorCodeOk;  // Yes! done.
    }

    DVLOG(2) << "Need to expand the record.";
    auto* record = target_->get_owner_id(match.index_);
    CHECK_ERROR_CODE(context_->sysxct_record_lock(sysxct_workspace, page_id, record));
    ASSERT_ND(record->is_keylocked());

    // Now the state of the record is finalized. Let's check it again.
    ASSERT_ND(!record->is_moved());  // can't be moved as target_ is not moved.
    if (target_->get_max_payload_length(match.index_) >= payload_count_) {
      return kErrorCodeOk;  // Yes! done.
    } else if (record->is_next_layer()) {
      DVLOG(0) << "Interesting. the record now points to next layer";
      return kErrorCodeOk;  // same kExactMatchLayerPointer
    }

    bool expanded = target_->try_expand_record_in_page_physical(payload_count_, match.index_);
    if (expanded) {
      ASSERT_ND(target_->get_max_payload_length(match.index_) >= payload_count_);
      return kErrorCodeOk;
    }

    DVLOG(0) << "Ouch. need to split for allocating a space for record expansion";
  } else if (match.match_type_ == MasstreeBorderPage::kConflictingLocalRecord) {
    // We will create a next layer.
    // In this case, page-lock was actually an overkill because of key-immutability,
    // However, doing it after page-lock makes the code simpler.
    // In most cases, the caller finds the conflicting local record before calling this
    // sysxct. No need to optimize for this rare case.
    ASSERT_ND(match.index_ < kBorderPageMaxSlots);

    auto* record = target_->get_owner_id(match.index_);
    CHECK_ERROR_CODE(context_->sysxct_record_lock(sysxct_workspace, page_id, record));
    ASSERT_ND(record->is_keylocked());

    // Now the state of the record is finalized. Let's check it again.
    ASSERT_ND(!record->is_moved());  // can't be moved as target_ is not moved.
    if (record->is_next_layer()) {
      DVLOG(0) << "Interesting. the record now points to next layer";
      return kErrorCodeOk;  // same kExactMatchLayerPointer
    }

    // Can we trivially turn this into a next-layer record?
    if (target_->get_max_payload_length(match.index_) < sizeof(DualPagePointer)) {
      DVLOG(1) << "We need to expand the record to make it a next-layer."
        " If this happens too often and is the bottleneck, "
        " you should have used physical_payload_hint when you initially inserted the record.";
      bool expanded
        = target_->try_expand_record_in_page_physical(sizeof(DualPagePointer), match.index_);
      if (expanded) {
        ASSERT_ND(target_->get_max_payload_length(match.index_) >= sizeof(DualPagePointer));
      }
    }

    if (target_->get_max_payload_length(match.index_) >= sizeof(DualPagePointer)) {
      // Turn it into a next-layer
      memory::NumaCoreMemory* memory = context_->get_thread_memory();
      memory::PagePoolOffset offset = memory->grab_free_volatile_page();
      if (offset == 0) {
        return kErrorCodeMemoryNoFreePages;
      }
      MasstreeBorderPage* new_layer_root = reinterpret_cast<MasstreeBorderPage*>(
        context_->get_local_volatile_page_resolver().resolve_offset_newpage(offset));
      VolatilePagePointer new_page_id;
      new_page_id.set(context_->get_numa_node(), offset);
      new_layer_root->initialize_as_layer_root_physical(new_page_id, target_, match.index_);
      return kErrorCodeOk;
    }

    DVLOG(0) << "Ouch. need to split for allocating a space for next-layer";
  } else {
    ASSERT_ND(match.match_type_ == MasstreeBorderPage::kNotFound);
    if (should_aggresively_create_next_layer_ &&
      target_->can_accomodate(key_count, sizeof(KeySlice), sizeof(DualPagePointer))) {
      DVLOG(1) << "Aggressively creating a next-layer.";

      MasstreeBorderPage* root = allocate_new_border_page(context_);
      if (root == nullptr) {
        return kErrorCodeMemoryNoFreePages;
      }
      DualPagePointer pointer;
      pointer.snapshot_pointer_ = 0;
      pointer.volatile_pointer_ = root->get_volatile_page_id();

      root->initialize_volatile_page(
        target_->header().storage_id_,
        pointer.volatile_pointer_,
        target_->get_layer() + 1U,
        kInfimumSlice,    // infimum slice
        kSupremumSlice);   // high-fence is supremum
      ASSERT_ND(!root->is_locked());
      ASSERT_ND(!root->is_moved());
      ASSERT_ND(!root->is_retired());
      ASSERT_ND(root->get_key_count() == 0);

      xct::XctId initial_id = get_initial_xid();
      initial_id.set_next_layer();
      target_->reserve_initially_next_layer(key_count, initial_id, slice_, pointer);

      assorted::memory_fence_release();
      target_->increment_key_count();
      ASSERT_ND(target_->does_point_to_layer(key_count));
      ASSERT_ND(target_->get_next_layer(key_count)->volatile_pointer_ == pointer.volatile_pointer_);

      ASSERT_ND(!target_->is_moved());
      ASSERT_ND(!target_->is_retired());
      target_->assert_entries();
      return kErrorCodeOk;
    }
    if (target_->can_accomodate(key_count, remainder_length_, payload_count_)) {
      ASSERT_ND(target_->get_key_count() < kBorderPageMaxSlots);
      xct::XctId initial_id = get_initial_xid();
      initial_id.set_deleted();
      target_->reserve_record_space(
        key_count,
        initial_id,
        slice_,
        suffix_,
        remainder_length_,
        payload_count_);
      // we increment key count AFTER installing the key because otherwise the optimistic read
      // might see the record but find that the key doesn't match. we need a fence to prevent it.
      assorted::memory_fence_release();
      target_->increment_key_count();
      ASSERT_ND(target_->get_key_count() <= kBorderPageMaxSlots);
      ASSERT_ND(!target_->is_moved());
      ASSERT_ND(!target_->is_retired());
      target_->assert_entries();
      return kErrorCodeOk;
    }

    DVLOG(1) << "Ouch. need to split for allocating a space for new record";
  }

  // If we are here, we need to split the page for some reason.
  out_split_needed_ = true;
  return kErrorCodeOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
