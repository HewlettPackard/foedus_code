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
#include "foedus/storage/masstree/masstree_adopt_impl.hpp"

#include <glog/logging.h>

#include "foedus/assert_nd.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_split_impl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

ErrorCode Adopt::run(xct::SysxctWorkspace* sysxct_workspace) {
  ASSERT_ND(!parent_->header().snapshot_);
  ASSERT_ND(!old_->header().snapshot_);
  ASSERT_ND(old_->is_moved());  // this is guaranteed because these flag are immutable once set.

  // Lock pages in one shot.
  Page* pages[2];
  pages[0] = reinterpret_cast<Page*>(parent_);
  pages[1] = reinterpret_cast<Page*>(old_);
  CHECK_ERROR_CODE(context_->sysxct_batch_page_locks(sysxct_workspace, 2, pages));

  // After the above lock, we check status of the pages
  if (parent_->is_moved()) {
    VLOG(0) << "Interesting. concurrent thread has already split this node?";
    return kErrorCodeOk;
  } else if (old_->is_retired()) {
    VLOG(0) << "Interesting. concurrent thread already adopted.";
    return kErrorCodeOk;
  }

  const KeySlice searching_slice = old_->get_low_fence();
  const auto minipage_index = parent_->find_minipage(searching_slice);
  auto& minipage = parent_->get_minipage(minipage_index);
  const auto pointer_index = minipage.find_pointer(searching_slice);
  ASSERT_ND(minipage.key_count_ <= kMaxIntermediateMiniSeparators);
  ASSERT_ND(old_->get_volatile_page_id() == minipage.pointers_[pointer_index].volatile_pointer_);

  // Now, how do we accommodate the new pointer?
  if (old_->get_foster_fence() == old_->get_low_fence()
    || old_->get_foster_fence() == old_->get_high_fence()) {
    // Case A. One of the grandchildrens is empty-range
    CHECK_ERROR_CODE(adopt_case_a(minipage_index, pointer_index));
  } else {
    // Case B. More complex, normal case
    CHECK_ERROR_CODE(adopt_case_b(minipage_index, pointer_index));
  }

  parent_->verify_separators();
  return kErrorCodeOk;
}

ErrorCode Adopt::adopt_case_a(
  uint16_t minipage_index,
  uint16_t pointer_index) {
  VLOG(0) << "Adopting from a child page that contains an empty-range page. This happens when"
    << " record compaction/expansion created a page without a record.";

  MasstreePage* grandchild_minor = context_->resolve_cast<MasstreePage>(old_->get_foster_minor());
  ASSERT_ND(grandchild_minor->get_low_fence() == old_->get_low_fence());
  ASSERT_ND(grandchild_minor->get_high_fence() == old_->get_foster_fence());
  MasstreePage* grandchild_major = context_->resolve_cast<MasstreePage>(old_->get_foster_major());
  ASSERT_ND(grandchild_major->get_low_fence() == old_->get_foster_fence());
  ASSERT_ND(grandchild_major->get_high_fence() == old_->get_high_fence());
  ASSERT_ND(!grandchild_minor->header().snapshot_);
  ASSERT_ND(!grandchild_major->header().snapshot_);

  ASSERT_ND(grandchild_minor->is_empty_range() != grandchild_major->is_empty_range());

  VolatilePagePointer nonempty_grandchild_pointer;
  MasstreePage* empty_grandchild = nullptr;
  MasstreePage* nonempty_grandchild = nullptr;
  if (grandchild_minor->is_empty_range()) {
    empty_grandchild = grandchild_minor;
    nonempty_grandchild = grandchild_major;
  } else {
    empty_grandchild = grandchild_major;
    nonempty_grandchild = grandchild_minor;
  }

  auto& minipage = parent_->get_minipage(minipage_index);
  ASSERT_ND(old_->get_volatile_page_id() == minipage.pointers_[pointer_index].volatile_pointer_);
  minipage.pointers_[pointer_index].snapshot_pointer_ = 0;
  minipage.pointers_[pointer_index].volatile_pointer_ = nonempty_grandchild->get_volatile_page_id();

  // The only thread that might be retiring this empty page must be in this function,
  // holding a page-lock in scope_child. Thus we don't need a lock in empty_grandchild.
  ASSERT_ND(!empty_grandchild->is_locked());  // none else holding lock on it
  // and we can safely retire the page. We do not use set_retired because is_moved() is false
  // It's a special retirement path.
  empty_grandchild->get_version_address()->status_.status_ |= PageVersionStatus::kRetiredBit;
  context_->collect_retired_volatile_page(empty_grandchild->get_volatile_page_id());
  old_->set_retired();
  context_->collect_retired_volatile_page(old_->get_volatile_page_id());
  return kErrorCodeOk;
}

ErrorCode Adopt::adopt_case_b(
  uint16_t minipage_index,
  uint16_t pointer_index) {
  const auto key_count = parent_->get_key_count();
  const KeySlice new_separator = old_->get_foster_fence();
  const VolatilePagePointer minor_pointer = old_->get_foster_minor();
  const VolatilePagePointer major_pointer = old_->get_foster_major();
  auto& minipage = parent_->get_minipage(minipage_index);
  ASSERT_ND(old_->get_volatile_page_id() == minipage.pointers_[pointer_index].volatile_pointer_);

  // Can we simply append the new pointer either as a new minipage at the end of this page
  // or as a new separator at the end of a minipage? Then we don't need major change.
  // We simply append without any change, which is easy to make right.
  if (minipage.key_count_ == pointer_index) {
    if (minipage.key_count_ < kMaxIntermediateMiniSeparators) {
      // We can append a new separator at the end of the minipage.
      ASSERT_ND(!minipage.pointers_[pointer_index].is_both_null());
      ASSERT_ND(pointer_index == 0 || minipage.separators_[pointer_index - 1] < new_separator);
      minipage.separators_[pointer_index] = new_separator;
      minipage.pointers_[pointer_index + 1].snapshot_pointer_ = 0;
      minipage.pointers_[pointer_index + 1].volatile_pointer_ = major_pointer;

      // we don't have to adopt the foster-minor because that's the child page itself,
      // but we have to switch the pointer
      minipage.pointers_[pointer_index].snapshot_pointer_ = 0;
      minipage.pointers_[pointer_index].volatile_pointer_ = minor_pointer;

      // we increase key count after above, with fence, so that concurrent transactions
      // never see an empty slot.
      assorted::memory_fence_release();
      ++minipage.key_count_;
      ASSERT_ND(minipage.key_count_ <= kMaxIntermediateMiniSeparators);

      old_->set_retired();
      context_->collect_retired_volatile_page(old_->get_volatile_page_id());
      return kErrorCodeOk;
    } else if (key_count == minipage_index && key_count < kMaxIntermediateSeparators) {
      // The minipage is full.. and the minipage is the last one!
      // We can add it as a new minipage
      auto& new_minipage = parent_->get_minipage(minipage_index + 1);
      new_minipage.key_count_ = 0;

#ifndef NDEBUG
      // for ease of debugging zero-out the page first (only data part). only for debug build.
      for (uint8_t j = 0; j <= kMaxIntermediateMiniSeparators; ++j) {
        if (j < kMaxIntermediateMiniSeparators) {
          new_minipage.separators_[j] = 0;
        }
        new_minipage.pointers_[j].snapshot_pointer_ = 0;
        new_minipage.pointers_[j].volatile_pointer_.word = 0;
      }
#endif  // NDEBUG

      ASSERT_ND(new_minipage.key_count_ == 0);
      new_minipage.pointers_[0].snapshot_pointer_ = 0;
      new_minipage.pointers_[0].volatile_pointer_ = major_pointer;

      // also handle foster-twin if it's border page
      DualPagePointer& old_pointer = minipage.pointers_[minipage.key_count_];
      old_pointer.snapshot_pointer_ = 0;
      old_pointer.volatile_pointer_ = minor_pointer;

      parent_->set_separator(minipage_index, new_separator);

      // increment key count after all with fence so that concurrent transactions never see
      // a minipage that is not ready for read
      assorted::memory_fence_release();
      parent_->increment_key_count();
      ASSERT_ND(parent_->get_key_count() == minipage_index + 1);

      old_->set_retired();
      context_->collect_retired_volatile_page(old_->get_volatile_page_id());
      return kErrorCodeOk;
    }
  }

  // In all other cases, we split this page.
  // We initially had more complex code to do in-page rebalance and
  // in-minipage "shifting", but got some bugs. Pulled out too many hairs.
  // Let's keep it simple. If we really observe bottleneck here, we can reconsider.
  // Splitting is way more robust because it changes nothing in this existing page.
  // It just places new foster-twin pointers.

  // Reuse SplitIntermediate. We are directly invoking the sysxct's internal logic
  // rather than nesting sysxct.
  memory::PagePoolOffset offsets[2];
  thread::GrabFreeVolatilePagesScope free_pages_scope(context_, offsets);
  CHECK_ERROR_CODE(free_pages_scope.grab(2));
  SplitIntermediate split(context_, parent_, old_);
  split.split_impl_no_error(&free_pages_scope);
  ASSERT_ND(old_->is_retired());  // the above internally retires old page
  return kErrorCodeOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
