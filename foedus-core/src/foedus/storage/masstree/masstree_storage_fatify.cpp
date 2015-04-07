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
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

#include <glog/logging.h>

#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"

namespace foedus {
namespace storage {
namespace masstree {

uint32_t count_children(const MasstreeIntermediatePage* page) {
  // this method assumes the page is locked. otherwise the following is not accurate.
  uint32_t current_count = 0;
  for (MasstreeIntermediatePointerIterator it(page); it.is_valid(); it.next()) {
    ++current_count;
  }
  return current_count;
}

struct Child {
  VolatilePagePointer pointer_;
  KeySlice            low_;
  KeySlice            high_;
  uint32_t            index_;
  uint32_t            index_mini_;
};

std::vector<Child> list_children(const MasstreeIntermediatePage* page) {
  std::vector<Child> ret;
  for (MasstreeIntermediatePointerIterator it(page); it.is_valid(); it.next()) {
    VolatilePagePointer pointer = it.get_pointer().volatile_pointer_;
    Child child = {pointer, it.get_low_key(), it.get_high_key(), it.index_, it.index_mini_};
    ASSERT_ND(!pointer.is_null());
    ret.emplace_back(child);
  }
  return ret;
}

ErrorStack MasstreeStoragePimpl::fatify_first_root(
  thread::Thread* context,
  uint32_t desired_count) {
  LOG(INFO) << "Masstree-" << get_name() << " being fatified for " << desired_count;

  if (desired_count > kMaxIntermediatePointers) {
    LOG(INFO) << "desired_count too large. adjusted to the max";
    desired_count = kMaxIntermediatePointers;
  }

  // Check if the volatile page is moved. If so, grow it.
  while (true) {
    MasstreeIntermediatePage* root;
    WRAP_ERROR_CODE(get_first_root(context, true, &root));

    if (root->has_foster_child()) {
      // oh, the root page needs to grow
      LOG(INFO) << "oh, the root page needs to grow";
      WRAP_ERROR_CODE(grow_root(
        context,
        &get_first_root_pointer(),
        &get_first_root_owner(),
        &root));
      // then retry
    } else {
      break;
    }
  }

  while (true) {
    // lock the first root.
    xct::McsLockScope owner_scope(context, &get_first_root_owner());
    LOG(INFO) << "Locked the root page owner address.";
    MasstreeIntermediatePage* root;
    WRAP_ERROR_CODE(get_first_root(context, true, &root));
    PageVersionLockScope scope(context, root->get_version_address());
    LOG(INFO) << "Locked the root page itself.";
    if (root->has_foster_child()) {
      LOG(WARNING) << "Mm, I thought I grew the root, but concurrent xct again moved it. "
        << " Gave up fatifying. Should be super-rare.";
      return kRetOk;
    }

    ASSERT_ND(root->is_locked());
    ASSERT_ND(!root->is_moved());
    uint32_t current_count = count_children(root);
    LOG(INFO) << "Masstree-" << get_name() << " currently has " << current_count << " children";

    if (current_count >= desired_count || current_count >= (kMaxIntermediatePointers / 2U)) {
      LOG(INFO) << "Already enough fat. Done";
      break;
    }

    LOG(INFO) << "Splitting...";
    CHECK_ERROR(fatify_first_root_double(context));

    WRAP_ERROR_CODE(get_first_root(context, true, &root));
    uint32_t new_count = count_children(root);
    if (new_count == current_count) {
      LOG(INFO) << "Seems like we can't split any more.";
      break;
    }
  }

  return kRetOk;
}

ErrorStack split_a_child(
  thread::Thread* context,
  MasstreeIntermediatePage* root,
  Child original,
  std::vector<Child>* out) {
  ASSERT_ND(!original.pointer_.is_null());
  MasstreeIntermediatePage::MiniPage& minipage = root->get_minipage(original.index_);
  ASSERT_ND(
    minipage.pointers_[original.index_mini_].volatile_pointer_.is_equivalent(original.pointer_));
  MasstreePage* original_page = context->resolve_cast<MasstreePage>(original.pointer_);
  ASSERT_ND(original_page->get_low_fence() == original.low_);
  ASSERT_ND(original_page->get_high_fence() == original.high_);

  // lock it first.
  PageVersionLockScope scope(context, original_page->get_version_address());
  ASSERT_ND(original_page->is_locked());

  // if it already has a foster child, nothing to do.
  if (!original_page->is_moved()) {
    if (original_page->is_border()) {
      MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(original_page);
      if (casted->get_key_count() < 2U) {
        // Then, no split possible.
        LOG(INFO) << "This border page can't be split anymore";
        out->emplace_back(original);
        return kRetOk;
      }
      // trigger doesn't matter. just make sure it doesn't cause no-record-split. so, use low_fence.
      KeySlice trigger = casted->get_low_fence();
      MasstreeBorderPage* after = casted;
      xct::McsBlockIndex after_lock;
      casted->split_foster(context, trigger, &after, &after_lock);
      ASSERT_ND(after->is_locked());
      context->mcs_release_lock(after->get_lock_address(), after_lock);
      ASSERT_ND(casted->is_moved());
    } else {
      MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(original_page);
      uint32_t pointers = count_children(casted);
      if (pointers < 2U) {
        LOG(INFO) << "This intermediate page can't be split anymore";
        out->emplace_back(original);
        return kRetOk;
      }
      WRAP_ERROR_CODE(casted->split_foster_no_adopt(context));
    }
  } else {
    LOG(INFO) << "lucky, already split. just adopt";
  }

  ASSERT_ND(original_page->is_moved());

  VolatilePagePointer minor_pointer = original_page->get_foster_minor();
  VolatilePagePointer major_pointer = original_page->get_foster_major();
  ASSERT_ND(!minor_pointer.is_null());
  ASSERT_ND(!major_pointer.is_null());
  MasstreePage* minor = context->resolve_cast<MasstreePage>(minor_pointer);
  MasstreePage* major = context->resolve_cast<MasstreePage>(major_pointer);
  KeySlice middle = original_page->get_foster_fence();
  ASSERT_ND(minor->get_low_fence() == original.low_);
  ASSERT_ND(minor->get_high_fence() == middle);
  ASSERT_ND(major->get_low_fence() == middle);
  ASSERT_ND(major->get_high_fence() == original.high_);

  Child minor_out = {minor_pointer, original.low_, middle, 0, 0};
  out->emplace_back(minor_out);
  Child major_out = {major_pointer, middle, original.high_, 0, 0};
  out->emplace_back(major_out);
  return kRetOk;
}

// defined here just for fatify code
void MasstreeIntermediatePage::split_foster_migrate_records_new_first_root(const void* arg) {
  ASSERT_ND(!header().snapshot_);
  ASSERT_ND(!is_moved());
  ASSERT_ND(!is_retired());
  ASSERT_ND(get_low_fence() == kInfimumSlice);
  ASSERT_ND(is_high_fence_supremum());
  ASSERT_ND(get_layer() == 0);
  ASSERT_ND(!is_border());

  const std::vector<Child>* new_children = reinterpret_cast< const std::vector<Child>* >(arg);
  // we have to mold the pointer list into IntermediateSplitStrategy.
  IntermediateSplitStrategy strategy;
  std::memset(&strategy, 0, sizeof(strategy));
  for (uint32_t i = 0; i < new_children->size(); ++i) {
    const Child& child = new_children->at(i);
    strategy.separators_[i] = child.high_;
    strategy.pointers_[i].volatile_pointer_ = child.pointer_;
  }
  strategy.total_separator_count_ = new_children->size();
  strategy.mid_separator_ = kSupremumSlice;
  strategy.mid_index_ = new_children->size();
  split_foster_migrate_records(strategy, 0, new_children->size(), kSupremumSlice);
}

void verify_new_root(
  thread::Thread* context,
  MasstreeIntermediatePage* new_root,
  const std::vector<Child>& new_children) {
  // this verification runs even in release mode. we must be super careful on root page.
  uint32_t count = new_children.size();
  uint32_t actual_count = count_children(new_root);
  ASSERT_ND(actual_count == count);
  if (actual_count != count) {
    LOG(FATAL) << "Child count doesn't match! expected=" << count << ", actual=" << actual_count;
  }

  ASSERT_ND(!new_root->is_border());
  ASSERT_ND(new_root->get_layer() == 0);
  ASSERT_ND(new_root->get_low_fence() == kInfimumSlice);
  ASSERT_ND(new_root->is_high_fence_supremum());

  uint32_t cur = 0;
  for (MasstreeIntermediatePointerIterator it(new_root); it.is_valid(); it.next()) {
    ASSERT_ND(it.get_pointer().snapshot_pointer_ == 0);
    VolatilePagePointer pointer = it.get_pointer().volatile_pointer_;
    ASSERT_ND(!pointer.is_null());
    if (pointer.is_null()) {
      LOG(FATAL) << "Nullptr? wtf";
    }

    const Child& child = new_children[cur];
    ASSERT_ND(pointer.is_equivalent(child.pointer_));
    ASSERT_ND(it.get_low_key() == child.low_);
    ASSERT_ND(it.get_high_key() == child.high_);
    if (!pointer.is_equivalent(child.pointer_)
        || it.get_low_key() != child.low_
        || it.get_high_key() != child.high_) {
      LOG(FATAL) << "Separator or pointer does not match!";
    }

    MasstreePage* page = context->resolve_cast<MasstreePage>(pointer);
    ASSERT_ND(page->get_low_fence() == child.low_);
    ASSERT_ND(page->get_high_fence() == child.high_);
    if (page->get_low_fence() != child.low_
      || page->get_high_fence() != child.high_) {
      LOG(FATAL) << "Fence key doesnt match!";
    }
    ++cur;
  }

  ASSERT_ND(cur == count);
}

ErrorStack MasstreeStoragePimpl::fatify_first_root_double(thread::Thread* context) {
  MasstreeIntermediatePage* root;
  WRAP_ERROR_CODE(get_first_root(context, true, &root));
  ASSERT_ND(root->is_locked());
  ASSERT_ND(!root->is_moved());

  // assure that all children have volatile version
  for (MasstreeIntermediatePointerIterator it(root); it.is_valid(); it.next()) {
    if (it.get_pointer().volatile_pointer_.is_null()) {
      MasstreePage* child;
      WRAP_ERROR_CODE(follow_page(
        context,
        true,
        const_cast<DualPagePointer*>(&it.get_pointer()),
        &child));
    }
    ASSERT_ND(!it.get_pointer().volatile_pointer_.is_null());
  }

  std::vector<Child> original_children = list_children(root);
  ASSERT_ND(original_children.size() * 2U <= kMaxIntermediatePointers);
  std::vector<Child> new_children;
  for (const Child& child : original_children) {
    CHECK_ERROR(split_a_child(context, root, child, &new_children));
  }
  ASSERT_ND(new_children.size() >= original_children.size());

  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset new_offset = memory->grab_free_volatile_page();
  if (new_offset == 0) {
    return ERROR_STACK(kErrorCodeMemoryNoFreePages);
  }
  // from now on no failure (we grabbed a free page).

  VolatilePagePointer new_pointer = combine_volatile_page_pointer(
    context->get_numa_node(),
    kVolatilePointerFlagSwappable,  // pointer to root page might be swapped!
    get_first_root_pointer().volatile_pointer_.components.mod_count + 1,
    new_offset);
  MasstreeIntermediatePage* new_root
    = context->resolve_newpage_cast<MasstreeIntermediatePage>(new_pointer);
  new_root->initialize_volatile_page(
    get_id(),
    new_pointer,
    0,
    root->get_btree_level(),  // same as current root. this is not grow_root
    kInfimumSlice,
    kSupremumSlice);
  // no concurrent access to the new page, but just for the sake of assertion in the func.
  PageVersionLockScope new_scope(context, new_root->get_version_address());
  new_root->split_foster_migrate_records_new_first_root(&new_children);
  ASSERT_ND(count_children(new_root) == new_children.size());
  verify_new_root(context, new_root, new_children);

  // set the new first-root pointer.
  assorted::memory_fence_release();
  get_first_root_pointer().volatile_pointer_.word = new_pointer.word;
  // first-root snapshot pointer is unchanged.

  // old root page and the direct children are now retired
  assorted::memory_fence_acq_rel();
  root->set_moved();  // not quite moved, but assertions assume that.
  root->set_retired();
  context->collect_retired_volatile_page(
    construct_volatile_page_pointer(root->header().page_id_));
  for (const Child& child : original_children) {
    MasstreePage* original_page = context->resolve_cast<MasstreePage>(child.pointer_);
    if (original_page->is_moved()) {
      PageVersionLockScope scope(context, original_page->get_version_address());
      original_page->set_retired();
      context->collect_retired_volatile_page(child.pointer_);
    } else {
      // This means, the page had too small records to split. We must keep it.
    }
  }
  assorted::memory_fence_acq_rel();

  LOG(INFO) << "Split done. " << original_children.size() << " -> " << new_children.size();

  return kRetOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
