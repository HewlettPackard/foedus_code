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
#include "foedus/storage/masstree/masstree_grow_impl.hpp"

#include <glog/logging.h>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/dumb_spinlock.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

// See Adopt for what is Case A/B.
void grow_case_a_common(
  thread::Thread* context,
  DualPagePointer* pointer,
  MasstreePage* cur_root) {
  ASSERT_ND(cur_root->is_locked());
  const auto& resolver = context->get_global_volatile_page_resolver();
  MasstreePage* minor
    = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(cur_root->get_foster_minor()));
  MasstreePage* major
    = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(cur_root->get_foster_major()));

  ASSERT_ND(minor->get_low_fence() == cur_root->get_low_fence());
  ASSERT_ND(minor->get_high_fence() == cur_root->get_foster_fence());
  ASSERT_ND(major->get_low_fence() == cur_root->get_foster_fence());
  ASSERT_ND(major->get_high_fence() == cur_root->get_high_fence());
  ASSERT_ND(!minor->header().snapshot_);
  ASSERT_ND(!major->header().snapshot_);
  ASSERT_ND(minor->is_empty_range() != major->is_empty_range());

  VolatilePagePointer nonempty_pointer;
  MasstreePage* empty_child;
  if (minor->is_empty_range()) {
    empty_child = minor;
    nonempty_pointer = cur_root->get_foster_major();
  } else {
    empty_child = major;
    nonempty_pointer = cur_root->get_foster_minor();
  }

  // snapshot pointer does NOT have to be reset. This is still logically the same page.
  pointer->volatile_pointer_ = nonempty_pointer;

  // Same as Adopt::adopt_case_a()
  ASSERT_ND(!empty_child->is_locked());
  empty_child->get_version_address()->status_.status_ |= PageVersionStatus::kRetiredBit;
  context->collect_retired_volatile_page(empty_child->get_volatile_page_id());
  cur_root->set_retired();
  context->collect_retired_volatile_page(cur_root->get_volatile_page_id());
}

ErrorCode grow_case_b_common(
  thread::Thread* context,
  DualPagePointer* pointer,
  MasstreePage* cur_root) {
  ASSERT_ND(cur_root->is_locked());
  const auto& resolver = context->get_global_volatile_page_resolver();

  // In this case, we create a new intermediate page.
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  VolatilePagePointer new_pointer = memory->grab_free_volatile_page_pointer();
  if (new_pointer.is_null()) {
    return kErrorCodeMemoryNoFreePages;
  }
  MasstreeIntermediatePage* new_root = reinterpret_cast<MasstreeIntermediatePage*>(
    resolver.resolve_offset_newpage(new_pointer));
  new_root->initialize_volatile_page(
    cur_root->header().storage_id_,
    new_pointer,
    cur_root->get_layer(),
    cur_root->get_btree_level() + 1U,
    kInfimumSlice,    // infimum slice
    kSupremumSlice);   // high-fence is supremum

  new_root->header().key_count_ = 0;
  auto& mini_page = new_root->get_minipage(0);
  mini_page.key_count_ = 1;
  mini_page.pointers_[0].snapshot_pointer_ = 0;
  mini_page.pointers_[0].volatile_pointer_ = cur_root->get_foster_minor();
  mini_page.pointers_[1].snapshot_pointer_ = 0;
  mini_page.pointers_[1].volatile_pointer_ = cur_root->get_foster_major();
  mini_page.separators_[0] = cur_root->get_foster_fence();

  // Let's install a pointer to the new root page
  assorted::memory_fence_release();  // must be after populating the new_root.
  // snapshot pointer does NOT have to be reset. This is still logically the same page.
  pointer->volatile_pointer_ = new_pointer;

  // the old root page is now retired
  cur_root->set_retired();
  context->collect_retired_volatile_page(cur_root->get_volatile_page_id());
  return kErrorCodeOk;
}

ErrorCode GrowFirstLayerRoot::run(xct::SysxctWorkspace* sysxct_workspace) {
  MasstreeStorage storage(context_->get_engine(), storage_id_);
  auto* cb = storage.get_control_block();

  // Take the special lock in control block.
  // This locking happens outside of the normal lock/commit protocol.
  // In order to avoid contention, we take the lock only when it looks available.
  if (cb->first_root_locked_) {
    DVLOG(0) << "Interesting. other thread seems growing the first-layer. let him do that";
    return kErrorCodeOk;
  }
  // Always a try-lock to avoid deadlock. Remember, this is a special lock!
  assorted::DumbSpinlock first_root_lock(&cb->first_root_locked_, false);
  if (!first_root_lock.try_lock()) {
    DVLOG(0) << "Interesting. other thread is growing the first-layer. let him do that";
    return kErrorCodeOk;
  }
  ASSERT_ND(first_root_lock.is_locked_by_me());

  const auto& resolver = context_->get_global_volatile_page_resolver();
  DualPagePointer* pointer = &cb->root_page_pointer_;
  ASSERT_ND(!pointer->volatile_pointer_.is_null());
  Page* cur_root_page = resolver.resolve_offset(pointer->volatile_pointer_);
  MasstreePage* cur_root = reinterpret_cast<MasstreePage*>(cur_root_page);
  ASSERT_ND(cur_root->is_layer_root());
  ASSERT_ND(cur_root->get_layer() == 0);

  CHECK_ERROR_CODE(context_->sysxct_page_lock(sysxct_workspace, cur_root_page));
  ASSERT_ND(cur_root->is_locked());
  // After locking the page, check the state of the page
  if (!cur_root->is_moved()) {
    DVLOG(0) << "Interesting. concurrent thread has already grown it.";
    return kErrorCodeOk;
  }

  ASSERT_ND(!cur_root->is_retired());
  if (cur_root->get_foster_fence() == cur_root->get_low_fence()
    || cur_root->get_foster_fence() == cur_root->get_high_fence()) {
    DVLOG(0) << "Adopting Empty-range child for first layer root. storage_id=" << storage_id_;
    grow_case_a_common(context_, pointer, cur_root);
  } else {
    LOG(INFO) << "Growing first layer root. storage_id=" << storage_id_;
    CHECK_ERROR_CODE(grow_case_b_common(context_, pointer, cur_root));
  }

  return kErrorCodeOk;
}

ErrorCode GrowNonFirstLayerRoot::run(xct::SysxctWorkspace* sysxct_workspace) {
  ASSERT_ND(parent_->is_border());
  ASSERT_ND(!parent_->header().snapshot_);
  // Once it becomes a next-layer pointer, it never goes back to a normal record, thus this is safe.
  ASSERT_ND(parent_->does_point_to_layer(pointer_index_));

  auto* record = parent_->get_owner_id(pointer_index_);
  auto parent_page_id = parent_->get_volatile_page_id();
  CHECK_ERROR_CODE(context_->sysxct_record_lock(sysxct_workspace, parent_page_id, record));
  ASSERT_ND(parent_->get_owner_id(pointer_index_)->is_keylocked());

  // After locking the record, check the state of the pointer
  if (record->is_moved()) {
    DVLOG(0) << "Interesting. concurrent thread has split or is splitting the parent page.";
    return kErrorCodeOk;  // no hurry. root-grow can be delayed
  }
  ASSERT_ND(!record->is_deleted());
  ASSERT_ND(record->is_next_layer());

  const auto& resolver = context_->get_global_volatile_page_resolver();
  DualPagePointer* pointer = parent_->get_next_layer(pointer_index_);
  ASSERT_ND(!pointer->volatile_pointer_.is_null());
  Page* cur_root_page = resolver.resolve_offset(pointer->volatile_pointer_);
  MasstreePage* cur_root = reinterpret_cast<MasstreePage*>(cur_root_page);
  ASSERT_ND(cur_root->is_layer_root());

  CHECK_ERROR_CODE(context_->sysxct_page_lock(sysxct_workspace, cur_root_page));
  ASSERT_ND(cur_root->is_locked());
  // After locking the page, check the state of the page
  if (!cur_root->is_moved()) {
    DVLOG(0) << "Interesting. concurrent thread has already grown it.";
    return kErrorCodeOk;
  }

  // But, is_retired is impossible because we locked the parent record before following
  ASSERT_ND(!cur_root->is_retired());
  if (cur_root->get_foster_fence() == cur_root->get_low_fence()
    || cur_root->get_foster_fence() == cur_root->get_high_fence()) {
    DVLOG(1) << "Easier. Empty-range foster child for non-first layer root."
      "storage_id=" << parent_->header().storage_id_;
    grow_case_a_common(context_, pointer, cur_root);
  } else {
    DVLOG(0) << "Growing non-first layer root. storage_id=" << parent_->header().storage_id_;
    CHECK_ERROR_CODE(grow_case_b_common(context_, pointer, cur_root));
  }

  return kErrorCodeOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
