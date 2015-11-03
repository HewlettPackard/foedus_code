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

#include <string>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"

namespace foedus {
namespace storage {
namespace masstree {

// Defines MasstreeStorage methods so that we can inline implementation calls
xct::TrackMovedRecordResult MasstreeStorage::track_moved_record(
  xct::RwLockableXctId* old_address,
  xct::WriteXctAccess* write_set) {
  return MasstreeStoragePimpl(this).track_moved_record(old_address, write_set);
}
ErrorStack MasstreeStoragePimpl::drop() {
  LOG(INFO) << "Uninitializing a masstree-storage " << get_name();

  if (control_block_->root_page_pointer_.volatile_pointer_.components.offset) {
    // release volatile pages
    const memory::GlobalVolatilePageResolver& page_resolver
      = engine_->get_memory_manager()->get_global_volatile_page_resolver();
    // first root is guaranteed to be an intermediate page
    MasstreeIntermediatePage* first_root = reinterpret_cast<MasstreeIntermediatePage*>(
      page_resolver.resolve_offset(control_block_->root_page_pointer_.volatile_pointer_));
    first_root->release_pages_recursive_parallel(engine_);
    control_block_->root_page_pointer_.volatile_pointer_.word = 0;
  }

  return kRetOk;
}

ErrorCode MasstreeStoragePimpl::get_first_root(
  thread::Thread* context,
  bool for_write,
  MasstreeIntermediatePage** root) {
  DualPagePointer* root_pointer = get_first_root_pointer_address();
  MasstreeIntermediatePage* page = nullptr;
  CHECK_ERROR_CODE(context->follow_page_pointer(
    nullptr,
    false,
    for_write,
    true,
    root_pointer,
    reinterpret_cast<Page**>(&page),
    nullptr,
    0));

  assert_aligned_page(page);
  ASSERT_ND(!for_write || !page->header().snapshot_);

  if (UNLIKELY(page->has_foster_child())) {
    ASSERT_ND(!page->header().snapshot_);
    ASSERT_ND(!get_first_root_owner().is_deleted());
    ASSERT_ND(!get_first_root_owner().is_moved());
    // root page has a foster child... time for tree growth!
    MasstreeIntermediatePage* new_root;
    CHECK_ERROR_CODE(grow_root(
      context,
      &get_first_root_pointer(),
      &get_first_root_owner(),
      &new_root));
    if (new_root) {
      assert_aligned_page(new_root);
      page = new_root;
    } else {
      // someone else has grown it. it's fine. we can still use the old page thanks to
      // the immutability
    }
  }

  *root = page;
  return kErrorCodeOk;
}


ErrorCode MasstreeStoragePimpl::grow_root(
  thread::Thread* context,
  DualPagePointer* root_pointer,
  xct::RwLockableXctId* root_pointer_owner,
  MasstreeIntermediatePage** new_root) {
  *new_root = nullptr;
  if (root_pointer_owner->is_keylocked()) {
    DVLOG(0) << "interesting. someone else is growing the tree, so let him do that.";
    // we can move on, thanks to the master-tree invariant. tree-growth is not a mandatory
    // task to do right away
    return kErrorCodeOk;
  }

  xct::McsRwLockScope owner_scope(context, root_pointer_owner, false);
  if (root_pointer_owner->is_moved()) {
    LOG(INFO) << "interesting. someone else has split the page that had a pointer"
      " to a root page of the layer to be grown";
    return kErrorCodeOk;  // same above.
  }

  // follow the pointer after taking lock on owner ID
  const memory::GlobalVolatilePageResolver& resolver = context->get_global_volatile_page_resolver();
  MasstreePage* root = reinterpret_cast<MasstreePage*>(
    resolver.resolve_offset(root_pointer->volatile_pointer_));
  if (root->get_layer() == 0) {
    LOG(INFO) << "growing B-tree in first layer! " << get_name();
  } else {
    DVLOG(0) << "growing B-tree in non-first layer " << get_name();
  }

  PageVersionLockScope scope(context, root->get_version_address());
  if (root->is_retired() || !root->has_foster_child()) {
    LOG(INFO) << "interesting. someone else has already grown B-tree";
    return kErrorCodeOk;  // retry. most likely we will see a new pointer
  }

  ASSERT_ND(root->is_locked());
  ASSERT_ND(root->has_foster_child());
  memory::NumaCoreMemory* memory = context->get_thread_memory();

  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  // from here no failure possible
  scope.set_changed();
  VolatilePagePointer new_pointer = combine_volatile_page_pointer(
    context->get_numa_node(),
    kVolatilePointerFlagSwappable,  // pointer to root page might be swapped!
    root_pointer->volatile_pointer_.components.mod_count + 1,
    offset);
  *new_root = reinterpret_cast<MasstreeIntermediatePage*>(
    resolver.resolve_offset_newpage(new_pointer));
  (*new_root)->initialize_volatile_page(
    get_id(),
    new_pointer,
    root->get_layer(),
    root->get_btree_level() + 1,
    kInfimumSlice,    // infimum slice
    kSupremumSlice);   // high-fence is supremum

  KeySlice separator = root->get_foster_fence();

  // the new root is not locked (no need), so we directly set key_count to avoid assertion.
  (*new_root)->header().key_count_ = 0;
  MasstreeIntermediatePage::MiniPage& mini_page = (*new_root)->get_minipage(0);
  MasstreePage* left_page
    = reinterpret_cast<MasstreePage*>(context->resolve(root->get_foster_minor()));
  MasstreePage* right_page
    = reinterpret_cast<MasstreePage*>(context->resolve(root->get_foster_major()));
  mini_page.key_count_ = 1;
  mini_page.pointers_[0].snapshot_pointer_ = 0;
  mini_page.pointers_[0].volatile_pointer_.word = left_page->header().page_id_;
  mini_page.pointers_[0].volatile_pointer_.components.flags = 0;
  ASSERT_ND(reinterpret_cast<Page*>(left_page) ==
    context->get_global_volatile_page_resolver().resolve_offset(
      mini_page.pointers_[0].volatile_pointer_));
  mini_page.pointers_[1].snapshot_pointer_ = 0;
  mini_page.pointers_[1].volatile_pointer_.word = right_page->header().page_id_;
  mini_page.pointers_[1].volatile_pointer_.components.flags = 0;
  ASSERT_ND(reinterpret_cast<Page*>(right_page) ==
    context->get_global_volatile_page_resolver().resolve_offset(
      mini_page.pointers_[1].volatile_pointer_));
  mini_page.separators_[0] = separator;
  ASSERT_ND(!(*new_root)->is_border());

  // Let's install a pointer to the new root page
  assorted::memory_fence_release();
  root_pointer->volatile_pointer_.word = new_pointer.word;
  if (root->get_layer() == 0) {
    LOG(INFO) << "Root of first layer is logically unchanged by grow_root, so the snapshot"
      << " pointer is unchanged. value=" << root_pointer->snapshot_pointer_;
  } else {
    root_pointer->snapshot_pointer_ = 0;
  }
  ASSERT_ND(reinterpret_cast<Page*>(*new_root) ==
    context->get_global_volatile_page_resolver().resolve_offset_newpage(
      root_pointer->volatile_pointer_));

  // the old root page is now retired
  root->set_retired();
  context->collect_retired_volatile_page(
    construct_volatile_page_pointer(root->header().page_id_));
  return kErrorCodeOk;
}
ErrorStack MasstreeStoragePimpl::load_empty() {
  control_block_->root_page_pointer_.snapshot_pointer_ = 0;
  control_block_->root_page_pointer_.volatile_pointer_.word = 0;
  control_block_->meta_.root_snapshot_page_id_ = 0;

  const uint16_t kDummyNode = 0;  // whatever. just pick from the first node
  memory::PagePool* pool
    = engine_->get_memory_manager()->get_node_memory(kDummyNode)->get_volatile_pool();
  const memory::LocalPageResolver &local_resolver = pool->get_resolver();

  // The root of first layer is always an intermediate page.
  // This is a special rule only for first layer to simplify partitioning and composer.
  memory::PagePoolOffset root_offset;
  WRAP_ERROR_CODE(pool->grab_one(&root_offset));
  ASSERT_ND(root_offset);
  MasstreeIntermediatePage* root_page = reinterpret_cast<MasstreeIntermediatePage*>(
    local_resolver.resolve_offset_newpage(root_offset));
  control_block_->root_page_pointer_.snapshot_pointer_ = 0;
  control_block_->root_page_pointer_.volatile_pointer_ = combine_volatile_page_pointer(
    kDummyNode,
    kVolatilePointerFlagSwappable,  // pointer to root page might be swapped!
    0,
    root_offset);
  root_page->initialize_volatile_page(
    get_id(),
    control_block_->root_page_pointer_.volatile_pointer_,
    0,
    1,
    kInfimumSlice,
    kSupremumSlice);

  // Also allocate the only child.
  memory::PagePoolOffset child_offset;
  WRAP_ERROR_CODE(pool->grab_one(&child_offset));
  ASSERT_ND(child_offset);
  MasstreeBorderPage* child_page = reinterpret_cast<MasstreeBorderPage*>(
    local_resolver.resolve_offset_newpage(child_offset));
  VolatilePagePointer child_pointer = combine_volatile_page_pointer(kDummyNode, 0, 0, child_offset);
  child_page->initialize_volatile_page(get_id(), child_pointer, 0, kInfimumSlice, kSupremumSlice);
  root_page->get_minipage(0).pointers_[0].snapshot_pointer_ = 0;
  root_page->get_minipage(0).pointers_[0].volatile_pointer_ = child_pointer;
  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::create(const MasstreeMetadata& metadata) {
  if (exists()) {
    LOG(ERROR) << "This masstree-storage already exists: " << get_name();
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  control_block_->meta_ = metadata;
  CHECK_ERROR(load_empty());
  control_block_->status_ = kExists;
  LOG(INFO) << "Newly created an masstree-storage " << get_name();
  return kRetOk;
}
ErrorStack MasstreeStoragePimpl::load(const StorageControlBlock& snapshot_block) {
  control_block_->meta_ = static_cast<const MasstreeMetadata&>(snapshot_block.meta_);
  const MasstreeMetadata& meta = control_block_->meta_;
  control_block_->root_page_pointer_.snapshot_pointer_ = meta.root_snapshot_page_id_;
  control_block_->root_page_pointer_.volatile_pointer_.word = 0;

  // So far we assume the root page always has a volatile version.
  // Create it now.
  if (meta.root_snapshot_page_id_ != 0) {
    cache::SnapshotFileSet fileset(engine_);
    CHECK_ERROR(fileset.initialize());
    UninitializeGuard fileset_guard(&fileset, UninitializeGuard::kWarnIfUninitializeError);
    VolatilePagePointer volatile_pointer;
    MasstreeIntermediatePage* volatile_root;
    CHECK_ERROR(engine_->get_memory_manager()->load_one_volatile_page(
      &fileset,
      meta.root_snapshot_page_id_,
      &volatile_pointer,
      reinterpret_cast<Page**>(&volatile_root)));
    CHECK_ERROR(fileset.uninitialize());
    volatile_pointer.components.flags = kVolatilePointerFlagSwappable;
    control_block_->root_page_pointer_.volatile_pointer_ = volatile_pointer;
  } else {
    LOG(INFO) << "This is an empty masstree: " << get_meta();
    CHECK_ERROR(load_empty());
  }

  control_block_->status_ = kExists;
  LOG(INFO) << "Loaded a masstree-storage " << get_meta();
  return kRetOk;
}

inline ErrorCode MasstreeStoragePimpl::find_border(
  thread::Thread* context,
  MasstreePage* layer_root,
  uint8_t   current_layer,
  bool      for_writes,
  KeySlice  slice,
  MasstreeBorderPage** border) {
  assert_aligned_page(layer_root);
  MasstreePage* cur = layer_root;
  cur->prefetch_general();
  while (true) {
    assert_aligned_page(cur);
    ASSERT_ND(cur->get_layer() == current_layer);
    ASSERT_ND(cur->within_fences(slice));
    if (UNLIKELY(cur->has_foster_child())) {
      // follow one of foster-twin.
      if (cur->within_foster_minor(slice)) {
        cur = reinterpret_cast<MasstreePage*>(context->resolve(cur->get_foster_minor()));
      } else {
        cur = reinterpret_cast<MasstreePage*>(context->resolve(cur->get_foster_major()));
      }
      ASSERT_ND(cur->within_fences(slice));
      continue;
    }

    if (cur->is_border()) {
      *border = reinterpret_cast<MasstreeBorderPage*>(cur);
      return kErrorCodeOk;
    } else {
      MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(cur);
      uint8_t minipage_index = page->find_minipage(slice);
      MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(minipage_index);

      minipage.prefetch();
      uint8_t pointer_index = minipage.find_pointer(slice);
      DualPagePointer& pointer = minipage.pointers_[pointer_index];
      MasstreePage* next;
      CHECK_ERROR_CODE(follow_page(context, for_writes, &pointer, &next));
      next->prefetch_general();
      if (next->within_fences(slice)) {
        if (next->has_foster_child() && !cur->is_moved()) {
          // oh, the page has foster child, so we should adopt it.
          CHECK_ERROR_CODE(page->adopt_from_child(
            context,
            slice,
            next));
          continue;  // we could keep going with a few cautions, but retrying is simpler.
        }
        cur = next;
      } else {
        // even in this case, local retry suffices thanks to foster-twin
        VLOG(0) << "Interesting. concurrent thread affected the search. local retry";
        assorted::memory_fence_acquire();
      }
    }
  }
}

ErrorCode MasstreeStoragePimpl::locate_record(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  bool for_writes,
  MasstreeBorderPage** out_page,
  SlotIndex* record_index,
  xct::XctId* observed) {
  ASSERT_ND(key_length <= kMaxKeyLength);
  MasstreePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(
    context,
    for_writes,
    reinterpret_cast<MasstreeIntermediatePage**>(&layer_root)));
  for (uint16_t current_layer = 0;; ++current_layer) {
    KeyLength remainder_length = key_length - current_layer * 8;
    KeySlice slice = slice_layer(key, key_length, current_layer);
    const void* suffix = reinterpret_cast<const char*>(key) + (current_layer + 1) * 8;
    MasstreeBorderPage* border;
    CHECK_ERROR_CODE(find_border(
      context,
      layer_root,
      current_layer,
      for_writes,
      slice,
      &border));
    PageVersionStatus border_version = border->get_version().status_;
    assorted::memory_fence_consume();
    SlotIndex index = border->find_key(slice, suffix, remainder_length);

    if (index == kBorderPageMaxSlots) {
      // this means not found. add it to page version set to protect the lack of record
      if (!border->header().snapshot_) {
        CHECK_ERROR_CODE(context->get_current_xct().add_to_page_version_set(
          border->get_version_address(),
          border_version));
      }
      // TASK(Hideaki) range lock rather than page-set to improve concurrency?
      return kErrorCodeStrKeyNotFound;
    }
    if (border->does_point_to_layer(index)) {
      CHECK_ERROR_CODE(follow_layer(context, for_writes, border, index, &layer_root));
      continue;
    } else {
      *out_page = border;
      *record_index = index;
      *observed = border->get_owner_id(index)->xct_id_;
      assorted::memory_fence_consume();
      return kErrorCodeOk;
    }
  }
}

ErrorCode MasstreeStoragePimpl::locate_record_normalized(
  thread::Thread* context,
  KeySlice key,
  bool for_writes,
  MasstreeBorderPage** out_page,
  SlotIndex* record_index,
  xct::XctId* observed) {
  MasstreeBorderPage* border;

  MasstreeIntermediatePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(context, for_writes, &layer_root));
  CHECK_ERROR_CODE(find_border(context, layer_root, 0, for_writes, key, &border));
  SlotIndex index = border->find_key_normalized(0, border->get_key_count(), key);
  PageVersionStatus border_version = border->get_version().status_;
  if (index == kBorderPageMaxSlots) {
    // this means not found
    if (!border->header().snapshot_) {
      CHECK_ERROR_CODE(context->get_current_xct().add_to_page_version_set(
        border->get_version_address(),
        border_version));
    }
    // TASK(Hideaki) range lock
    return kErrorCodeStrKeyNotFound;
  }
  // because this is just one slice, we never go to second layer
  ASSERT_ND(!border->does_point_to_layer(index));
  *out_page = border;
  *record_index = index;
  *observed = border->get_owner_id(index)->xct_id_;
  assorted::memory_fence_consume();
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::create_next_layer(
  thread::Thread* context,
  MasstreeBorderPage* parent,
  SlotIndex parent_index) {
  // This method assumes that the record's payload space is spacious enough.
  // The caller must make it sure as pre-condition.
  ASSERT_ND(parent->get_max_payload_length(parent_index) >= sizeof(DualPagePointer));
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  const memory::LocalPageResolver &resolver = context->get_local_volatile_page_resolver();
  MasstreeBorderPage* root = reinterpret_cast<MasstreeBorderPage*>(
    resolver.resolve_offset_newpage(offset));
  DualPagePointer pointer;
  pointer.snapshot_pointer_ = 0;
  pointer.volatile_pointer_ = combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offset);

  xct::RwLockableXctId* parent_lock = parent->get_owner_id(parent_index);

  // as an independent system transaction, here we do an optimistic version check.
  xct::McsRwLockScope parent_scope(context, parent_lock, false);
  if (parent_lock->is_moved() || parent->does_point_to_layer(parent_index)) {
    // someone else has also made this to a next layer or the page itself is moved!
    // our effort was a waste, but anyway the goal was achieved.
    VLOG(0) << "interesting. a concurrent thread has already made this page moved "
      << " or moved the record to next layer";
    memory->release_free_volatile_page(offset);
  } else {
    // initialize the root page by copying the record
    root->initialize_volatile_page(
      get_id(),
      pointer.volatile_pointer_,
      parent->get_layer() + 1,
      kInfimumSlice,    // infimum slice
      kSupremumSlice);   // high-fence is supremum
    ASSERT_ND(!root->is_locked());
    root->initialize_layer_root(parent, parent_index);
    ASSERT_ND(!root->is_moved());
    ASSERT_ND(!root->is_retired());

    MasstreeBorderPage::Slot* slot = parent->get_slot(parent_index);
    ASSERT_ND(!slot->does_point_to_layer());
    MasstreeBorderPage::SlotLengthPart new_lengthes = slot->lengthes_.components;
    new_lengthes.payload_length_ = sizeof(DualPagePointer);

    char* parent_payload = parent->get_record_payload(parent_index);

    // point to the new page. Be careful on ordering.
    std::memcpy(parent_payload, &pointer, sizeof(pointer));
    assorted::memory_fence_release();
    slot->write_lengthes_oneshot(new_lengthes);
    assorted::memory_fence_release();
    parent_lock->xct_id_.set_next_layer();  // which also turns off delete-bit

    ASSERT_ND(parent->get_next_layer(parent_index)->volatile_pointer_.components.offset == offset);
    ASSERT_ND(parent->get_next_layer(parent_index)->volatile_pointer_.components.numa_node
      == context->get_numa_node());

    // change ordinal just to let concurrent transactions get aware
    assorted::memory_fence_release();
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::follow_page(
  thread::Thread* context,
  bool for_writes,
  storage::DualPagePointer* pointer,
  MasstreePage** page) {
  return context->follow_page_pointer(
    nullptr,  // masstree doesn't create a new page except splits.
    false,  // so, there is no null page possible
    for_writes,  // always get volatile pages for writes
    true,
    pointer,
    reinterpret_cast<Page**>(page),
    nullptr,  // only used for new page creation, so nothing to pass
    -1);  // same as above
}

inline ErrorCode MasstreeStoragePimpl::follow_layer(
  thread::Thread* context,
  bool for_writes,
  MasstreeBorderPage* parent,
  SlotIndex record_index,
  MasstreePage** page) {
  ASSERT_ND(record_index < kBorderPageMaxSlots);
  ASSERT_ND(parent->does_point_to_layer(record_index));
  DualPagePointer* pointer = parent->get_next_layer(record_index);
  xct::RwLockableXctId* owner = parent->get_owner_id(record_index);
  ASSERT_ND(owner->xct_id_.is_next_layer() || owner->xct_id_.is_moved());
  ASSERT_ND(!pointer->is_both_null());
  MasstreePage* next_root;
  CHECK_ERROR_CODE(follow_page(context, for_writes, pointer, &next_root));

  // root page has a foster child... time for tree growth!
  if (UNLIKELY(next_root->has_foster_child())) {
    MasstreeIntermediatePage* new_next_root;
    CHECK_ERROR_CODE(grow_root(context, pointer, owner, &new_next_root));
    if (new_next_root) {
      next_root = new_next_root;
    } else {
      // someone else has grown it. it's fine. we can still use the old page thanks to
      // the immutability
    }
  }

  ASSERT_ND(next_root);
  *page = next_root;
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::lock_and_expand_record(
  thread::Thread* context,
  PayloadLength required_payload_count,
  MasstreeBorderPage* border,
  SlotIndex record_index) {
  PageVersionLockScope scope(context, border->get_version_address());
  // Check the condition again after locking.
  if (border->is_moved()) {
    VLOG(0) << "Interesting. now it's split";
    return kErrorCodeOk;
  } else if (border->does_point_to_layer(record_index)) {
    VLOG(0) << "Interesting. now it's pointing to next layer";
    return kErrorCodeOk;
  } else if (border->get_max_payload_length(record_index) >= required_payload_count) {
    VLOG(0) << "Interesting. The record is already expanded";
    return kErrorCodeOk;
  }

  return expand_record(
    context,
    required_payload_count,
    border,
    record_index,
    &scope);
}


ErrorCode MasstreeStoragePimpl::expand_record(
  thread::Thread* context,
  PayloadLength physical_payload_hint,
  MasstreeBorderPage* border,
  SlotIndex record_index,
  PageVersionLockScope* lock_scope) {
  ASSERT_ND(border->is_locked());
  ASSERT_ND(!lock_scope->released_);
  ASSERT_ND(!border->is_moved());
  ASSERT_ND(!border->does_point_to_layer(record_index));
  ASSERT_ND(record_index < border->get_key_count());
  DVLOG(2) << "Expanding record.. current max=" << border->get_max_payload_length(record_index)
    << ", which must become " << physical_payload_hint;

  ASSERT_ND(border->verify_slot_lengthes(record_index));
  MasstreeBorderPage::Slot* slot = border->get_slot(record_index);
  ASSERT_ND(!slot->tid_.is_moved());
  const MasstreeBorderPage::SlotLengthPart lengthes = slot->lengthes_.components;
  const KeyLength remainder_length = slot->remainder_length_;
  const DataOffset record_length = MasstreeBorderPage::to_record_length(
    remainder_length,
    physical_payload_hint);
  const DataOffset available = border->available_space();

  // 1. Trivial expansion if the record is placed at last. Fastest.
  if (border->get_next_offset() == lengthes.offset_ + lengthes.physical_record_length_) {
    const DataOffset diff = record_length - lengthes.physical_record_length_;
    DVLOG(1) << "Lucky, expanding a record at last record region. diff=" << diff;
    if (available >= diff) {
      DVLOG(2) << "woo. yes, we can just increase the length";
      slot->lengthes_.components.physical_record_length_ = record_length;
      border->increase_next_offset(diff);
      return kErrorCodeOk;
    }
  }

  // 2. In-page expansion. Fast.
  if (available >= record_length) {
    DVLOG(2) << "Okay, in-page record expansion.";
    // We have to make sure all threads see a valid state, either new or old.
    MasstreeBorderPage::SlotLengthPart new_lengthes = lengthes;
    new_lengthes.offset_ = border->get_next_offset();
    new_lengthes.physical_record_length_ = record_length;
    const char* old_record = border->get_record_from_offset(lengthes.offset_);
    char* new_record = border->get_record_from_offset(new_lengthes.offset_);

    // 2-a. Create the new record region.
    if (lengthes.physical_record_length_ > 0) {
      std::memcpy(new_record, old_record, lengthes.physical_record_length_);
    }

    // 2-b. Lock the record and announce the new location in one-shot.
    {
      xct::McsRwLockScope record_lock(context, &slot->tid_, false);
      // The above lock implies a barrier here
      ASSERT_ND(!slot->tid_.is_moved());
      slot->write_lengthes_oneshot(new_lengthes);
      assorted::memory_fence_release();
      // We don't have to change TID here because we did nothing logically.
      // Reading transactions are safe to read either old or new record regions.
      // See comments in MasstreeCommonLogType::apply_record_prepare() for how we make it safe
      // for writing transactions.
    }

    // Above unlock implies a barrier here
    border->increase_next_offset(record_length);
    return kErrorCodeOk;
  }

  // 3. ouch. by far slowest
  DVLOG(1) << "Umm, we need to split this page for record expansion. available="
    << available << ", record_length=" << record_length
    << ", record_index=" << record_index
    << ", key_count=" << border->get_key_count();

  KeySlice slice = border->get_slice(record_index);
  MasstreeBorderPage* new_child;
  xct::McsLockScope new_child_lock;
  ASSERT_ND(!new_child_lock.is_locked());
  CHECK_ERROR_CODE(border->split_foster(
    context,
    slice,
    true,  // we are splitting to make room. disable no-record-split
    &new_child,
    &new_child_lock));
  ASSERT_ND(new_child->is_locked());
  ASSERT_ND(new_child_lock.is_locked());
  ASSERT_ND(new_child->within_fences(slice));
  ASSERT_ND(border->is_moved());
  ASSERT_ND(border->is_locked());
  ASSERT_ND(!lock_scope->released_);

  // Then recurse to the new child to expand. What's the new index?
  const char* suffix = border->get_record(record_index);
  SlotIndex new_index = new_child->find_key(slice, suffix, remainder_length);
  ASSERT_ND(new_index < new_child->get_key_count());
  if (new_index >= new_child->get_key_count()) {
    // The new page is still locked by myself. This must not happen.
    LOG(ERROR) << "Couldn't find the record in new page. This must not happen.";
  }

  // Convert McsLockScope to PageVersionLockScope. this is a tentative solution
  PageVersionLockScope new_child_page_scope(&new_child_lock);
  ASSERT_ND(!new_child_lock.is_locked());
  ASSERT_ND(new_child_page_scope.block_ != 0);
  CHECK_ERROR_CODE(expand_record(
    context,
    physical_payload_hint,
    new_child,
    new_index,
    &new_child_page_scope));
  DVLOG(2) << "Expanded record in new child";

  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::reserve_record(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  PayloadLength payload_count,
  PayloadLength physical_payload_hint,
  MasstreeBorderPage** out_page,
  SlotIndex* record_index,
  xct::XctId* observed) {
  ASSERT_ND(key_length <= kMaxKeyLength);
  ASSERT_ND(physical_payload_hint >= payload_count);

  MasstreePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(
    context,
    true,
    reinterpret_cast<MasstreeIntermediatePage**>(&layer_root)));
  for (uint16_t layer = 0;; ++layer) {
    const KeyLength remainder = key_length - layer * sizeof(KeySlice);
    const KeySlice slice = slice_layer(key, key_length, layer);
    const void* const suffix = reinterpret_cast<const char*>(key) + (layer + 1) * sizeof(KeySlice);
    MasstreeBorderPage* border;
    CHECK_ERROR_CODE(find_border(context, layer_root, layer, true, slice, &border));
    while (true) {  // retry loop for following foster child and temporary failure
      // if we found out that the page was split and we should follow foster child, do it.
      while (border->has_foster_child()) {
        if (border->within_foster_minor(slice)) {
          border = context->resolve_cast<MasstreeBorderPage>(border->get_foster_minor());
        } else {
          border = context->resolve_cast<MasstreeBorderPage>(border->get_foster_major());
        }
      }
      ASSERT_ND(border->within_fences(slice));

      SlotIndex count = border->get_key_count();
      // as done in reserve_record_new_record_apply(), we need a fence on BOTH sides.
      // observe key count first, then verify the keys.
      assorted::memory_fence_consume();
      MasstreeBorderPage::FindKeyForReserveResult match = border->find_key_for_reserve(
        0,
        count,
        slice,
        suffix,
        remainder);

      if (match.match_type_ == MasstreeBorderPage::kExactMatchLayerPointer) {
        ASSERT_ND(match.index_ < kBorderPageMaxSlots);
        CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
        break;  // next layer
      } else if (match.match_type_ == MasstreeBorderPage::kExactMatchLocalRecord) {
        // Even in this case, if the record space is too small, we must migrate it first.
        // This is another system transaction. After that, we retry.
        if (border->get_max_payload_length(match.index_) < payload_count) {
          // We haven't locked the page yet, so use lock_and_expand_record.
          // Hopefully record expansion is not that often, so this shouldn't matter
          CHECK_ERROR_CODE(lock_and_expand_record(
            context,
            physical_payload_hint,
            border,
            match.index_));
          continue;  // must retry no matter what happened.
        }
        *out_page = border;
        *record_index = match.index_;
        *observed = border->get_owner_id(match.index_)->xct_id_;
        if (observed->is_next_layer()) {
          // because the search is optimistic, we might now see a XctId with next-layer bit on.
          // in this case, we retry.
          VLOG(0) << "Interesting. Next-layer-retry due to concurrent transaction";
          continue;
        }
        assorted::memory_fence_consume();
        return kErrorCodeOk;
      } else if (match.match_type_ == MasstreeBorderPage::kConflictingLocalRecord) {
        // in this case, we don't need a page-wide lock. because of key-immutability,
        // this is the only place we can have a next-layer pointer for this slice.
        // thus we just lock the record and convert it to a next-layer pointer.
        ASSERT_ND(match.index_ < kBorderPageMaxSlots);

        // this means now we have to create a next layer.
        // this is also one system transaction.

        // Can we trivially turn this into a next-layer record?
        if (border->get_max_payload_length(match.index_) < sizeof(DualPagePointer)) {
          // Same as above.
          CHECK_ERROR_CODE(lock_and_expand_record(
            context,
            sizeof(DualPagePointer),
            border,
            match.index_));
          continue;  // must retry no matter what happened.
        }

        CHECK_ERROR_CODE(create_next_layer(context, border, match.index_));
        // because we do this without page lock, this might have failed. in that case,
        // we retry.
        if (border->does_point_to_layer(match.index_)) {
          ASSERT_ND(border->get_owner_id(match.index_)->xct_id_.is_next_layer());
          CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
          break;  // next layer
        } else {
          VLOG(0) << "Because of concurrent transaction, we retry the system transaction to"
            << " make the record into a next layer pointer";
          continue;
        }
      } else {
        ASSERT_ND(match.match_type_ == MasstreeBorderPage::kNotFound);
      }

      // no matching or conflicting keys. so we will create a brand new record.
      // this is a system transaction to just create a deleted record.
      // for this, we need a page lock.
      PageVersionLockScope scope(context, border->get_version_address());
      border->assert_entries();
      // now finally we took a lock, finalizing the version. up to now, everything could happen.
      // check all of them and retry if fails.
      if (UNLIKELY(border->is_moved())) {
        continue;  // locally retries
      }
      // even resume the searches if new record was installed (only new record area)
      if (count != border->get_key_count()) {
        ASSERT_ND(count < border->get_key_count());
        // someone else has inserted a new record. Is it conflicting?
        // search again, but only for newly inserted record(s)
        SlotIndex new_count = border->get_key_count();
        match = border->find_key_for_reserve(count, new_count, slice, suffix, remainder);
        count = new_count;
      }

      if (LIKELY(match.match_type_ == MasstreeBorderPage::kNotFound)) {
        // okay, surely new record. Should be this case unless there is a race.
        scope.set_changed();
        if (get_meta().should_aggresively_create_next_layer(layer, remainder)) {
          // min_layer_hint_ configuration tells that we should start with a next-layer record
          // rather than creating a usual record and then moving it later.
          DVLOG(1) << "Aggressively creating a next-layer.";
          CHECK_ERROR_CODE(reserve_record_next_layer(context, border, slice, &layer_root));
          ASSERT_ND(layer_root->get_layer() == layer + 1U);
          break;  // next layer
        }

        CHECK_ERROR_CODE(reserve_record_new_record(
          context,
          border,
          slice,
          remainder,
          suffix,
          physical_payload_hint,
          &scope,
          out_page,
          record_index,
          observed));
        ASSERT_ND(!scope.released_);
        ASSERT_ND(!(*out_page)->is_moved());
        ASSERT_ND((*out_page)->is_locked());
        ASSERT_ND((*out_page)->get_version_address() == scope.version_);
        ASSERT_ND(*record_index < (*out_page)->get_key_count());
        return kErrorCodeOk;
      } else {
        // This is rare, thus for code simplicity we just retry.
        // It's a bit wasteful as we already know the exact match index,
        // but let's optimize corner cases after we see bottlenecks here.
        VLOG(1) << "Interesting. Now found something inserted by concurrent thread. "
          << "match_type_=" << match.match_type_;
        continue;  // retry
      }
    }
  }
}

ErrorCode MasstreeStoragePimpl::reserve_record_normalized(
  thread::Thread* context,
  KeySlice key,
  PayloadLength payload_count,
  PayloadLength physical_payload_hint,
  MasstreeBorderPage** out_page,
  SlotIndex* record_index,
  xct::XctId* observed) {
  MasstreeBorderPage* border;

  MasstreeIntermediatePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(context, true, &layer_root));
  CHECK_ERROR_CODE(find_border(context, layer_root, 0, true, key, &border));
  while (true) {  // retry loop for following foster child
    // if we found out that the page was split and we should follow foster child, do it.
    while (border->has_foster_child()) {
      if (border->within_foster_minor(key)) {
        border = context->resolve_cast<MasstreeBorderPage>(border->get_foster_minor());
      } else {
        border = context->resolve_cast<MasstreeBorderPage>(border->get_foster_major());
      }
    }

    PageVersionLockScope scope(context, border->get_version_address());
    border->assert_entries();
    if (UNLIKELY(border->has_foster_child())) {
      continue;
    }
    ASSERT_ND(!border->has_foster_child());
    ASSERT_ND(!border->is_moved());
    ASSERT_ND(border->get_foster_major().is_null());
    ASSERT_ND(border->get_foster_minor().is_null());
    ASSERT_ND(border->within_fences(key));

    // because we never go on to second layer in this case, it's either a full match or not-found
    SlotIndex count = border->get_key_count();
    SlotIndex index = border->find_key_normalized(0, count, key);

    if (index != kBorderPageMaxSlots) {
      // If the record space is too small, we can't insert.
      if (border->get_max_payload_length(index) < payload_count) {
        CHECK_ERROR_CODE(expand_record(
          context,
          physical_payload_hint,
          border,
          index,
          &scope));
        continue;  // retry (will see foster child)
      }
      scope.release();
      *out_page = border;
      *record_index = index;
      *observed = border->get_owner_id(index)->xct_id_;
      assorted::memory_fence_consume();
      return kErrorCodeOk;
    }

    scope.set_changed();
    return reserve_record_new_record(
      context,
      border,
      key,
      sizeof(KeySlice),
      nullptr,
      physical_payload_hint,
      &scope,
      out_page,
      record_index,
      observed);
  }
}

ErrorCode MasstreeStoragePimpl::reserve_record_new_record(
  thread::Thread* context,
  MasstreeBorderPage* border,
  KeySlice key,
  KeyLength remainder,
  const void* suffix,
  PayloadLength payload_count,
  PageVersionLockScope* out_page_lock,
  MasstreeBorderPage** out_page,
  SlotIndex* record_index,
  xct::XctId* observed) {
  // First, split as many times as needed until we can insert the record to this page.
  *out_page = nullptr;
  while (true) {
    ASSERT_ND(!out_page_lock->released_);
    ASSERT_ND(out_page_lock->version_ == border->get_version_address());
    ASSERT_ND(border->is_locked());
    ASSERT_ND(!border->is_moved());
    ASSERT_ND(border->get_foster_major().is_null());
    ASSERT_ND(border->get_foster_minor().is_null());
    SlotIndex count = border->get_key_count();
    bool early_split = border->should_split_early(count, get_meta().border_early_split_threshold_);
    if (early_split || !border->can_accomodate(count, remainder, payload_count)) {
      // have to split to make room. the newly created foster child is always the place to insert.
      if (early_split) {
        DVLOG(1) << "Early split! cur count=" << count << ", storage=" << get_name();
      }

      MasstreeBorderPage* target;
      xct::McsLockScope target_lock;
      ASSERT_ND(!target_lock.is_locked());
      CHECK_ERROR_CODE(border->split_foster(context, key, false, &target, &target_lock));
      ASSERT_ND(target->is_locked());
      ASSERT_ND(target_lock.is_locked());
      ASSERT_ND(target->within_fences(key));
      ASSERT_ND(target->find_key(key, suffix, remainder) == kBorderPageMaxSlots);

      // go down and keep splitting. out_page_lock now also points to the new target.
      // Convert McsLockScope to PageVersionLockScope. this is a tentative solution
      PageVersionLockScope new_scope(&target_lock);
      out_page_lock->take_over(&new_scope);
      ASSERT_ND(!target_lock.is_locked());
      ASSERT_ND(new_scope.released_);
      ASSERT_ND(!out_page_lock->released_);
      ASSERT_ND(out_page_lock->version_ == target->get_version_address());
      border = target;
    } else {
      break;  // no need to split
    }
  }

  ASSERT_ND(border->is_locked());
  ASSERT_ND(!border->is_moved());
  SlotIndex count = border->get_key_count();
  ASSERT_ND(border->can_accomodate(count, remainder, payload_count));
  ASSERT_ND(!border->should_split_early(count, get_meta().border_early_split_threshold_));
  reserve_record_new_record_apply(
    context,
    border,
    count,
    key,
    remainder,
    suffix,
    payload_count,
    observed);
  *out_page = border;
  *record_index = count;
  return kErrorCodeOk;
}

void MasstreeStoragePimpl::reserve_record_new_record_apply(
  thread::Thread* /*context*/,
  MasstreeBorderPage* target,
  SlotIndex target_index,
  KeySlice slice,
  KeyLength remainder_length,
  const void* suffix,
  PayloadLength payload_count,
  xct::XctId* observed) {
  ASSERT_ND(target->is_locked());
  ASSERT_ND(!target->is_moved());
  ASSERT_ND(!target->is_retired());
  ASSERT_ND(target->can_accomodate(target_index, remainder_length, payload_count));
  ASSERT_ND(target->get_key_count() < kBorderPageMaxSlots);
  xct::XctId initial_id;
  initial_id.set(
    Epoch::kEpochInitialCurrent,  // TODO(Hideaki) this should be something else
    0);
  initial_id.set_deleted();
  *observed = initial_id;
  target->reserve_record_space(
    target_index,
    initial_id,
    slice,
    suffix,
    remainder_length,
    payload_count);
  // we increment key count AFTER installing the key because otherwise the optimistic read
  // might see the record but find that the key doesn't match. we need a fence to prevent it.
  assorted::memory_fence_release();
  target->increment_key_count();
  ASSERT_ND(target->get_key_count() <= kBorderPageMaxSlots);
  ASSERT_ND(!target->is_moved());
  ASSERT_ND(!target->is_retired());
  target->assert_entries();
}

ErrorCode MasstreeStoragePimpl::reserve_record_next_layer(
  thread::Thread* context,
  MasstreeBorderPage* border,
  KeySlice slice,
  MasstreePage** out_page) {
  ASSERT_ND(border->is_locked());
  ASSERT_ND(!border->is_moved());
  ASSERT_ND(border->get_foster_major().is_null());
  ASSERT_ND(border->get_foster_minor().is_null());
  SlotIndex count = border->get_key_count();
  bool early_split = border->should_split_early(count, get_meta().border_early_split_threshold_);
  if (!early_split && border->can_accomodate(count, sizeof(KeySlice), sizeof(DualPagePointer))) {
    CHECK_ERROR_CODE(reserve_record_next_layer_apply(context, border, slice, out_page));
  } else {
#ifndef NDEBUG
    if (early_split) {
      DVLOG(1) << "Early split! cur count=" << count << ", storage=" << get_name();
    }
#endif  // NDEBUG
    // have to split to make room. the newly created foster child is always the place to insert.
    MasstreeBorderPage* target;
    xct::McsLockScope target_lock;
    ASSERT_ND(!target_lock.is_locked());
    CHECK_ERROR_CODE(border->split_foster(context, slice, false, &target, &target_lock));
    ASSERT_ND(target->is_locked());
    ASSERT_ND(target_lock.is_locked());
    ASSERT_ND(target->within_fences(slice));
    count = target->get_key_count();
    if (!target->can_accomodate(count, sizeof(KeySlice), sizeof(DualPagePointer))) {
      // this might happen if payload_count is huge. so far just error out.
      LOG(WARNING) << "Wait, not enough space even after splits? should be pretty rare...";
      return kErrorCodeStrTooLongPayload;
    }
    CHECK_ERROR_CODE(reserve_record_next_layer_apply(context, target, slice, out_page));
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::reserve_record_next_layer_apply(
  thread::Thread* context,
  MasstreeBorderPage* target,
  KeySlice slice,
  MasstreePage** out_page) {
  ASSERT_ND(target->is_locked());
  ASSERT_ND(!target->is_moved());
  ASSERT_ND(!target->is_retired());
  const SlotIndex count = target->get_key_count();
  ASSERT_ND(target->can_accomodate(count, sizeof(KeySlice), sizeof(DualPagePointer)));
  xct::XctId initial_id;
  initial_id.set(
    Epoch::kEpochInitialCurrent,  // TODO(Hideaki) this should be something else
    0);
  initial_id.set_next_layer();

  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  const memory::LocalPageResolver &resolver = context->get_local_volatile_page_resolver();
  MasstreeBorderPage* root = reinterpret_cast<MasstreeBorderPage*>(
    resolver.resolve_offset_newpage(offset));
  DualPagePointer pointer;
  pointer.snapshot_pointer_ = 0;
  pointer.volatile_pointer_ = combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offset);

  // initialize the root page by copying the record
  root->initialize_volatile_page(
    get_id(),
    pointer.volatile_pointer_,
    target->get_layer() + 1U,
    kInfimumSlice,    // infimum slice
    kSupremumSlice);   // high-fence is supremum
  ASSERT_ND(!root->is_locked());
  ASSERT_ND(!root->is_moved());
  ASSERT_ND(!root->is_retired());
  ASSERT_ND(root->get_key_count() == 0);

  assorted::memory_fence_release();
  target->reserve_initially_next_layer(count, initial_id, slice, pointer);
  assorted::memory_fence_release();
  target->increment_key_count();
  ASSERT_ND(target->does_point_to_layer(count));
  ASSERT_ND(target->get_next_layer(count)->
    volatile_pointer_.is_equivalent(pointer.volatile_pointer_));

  ASSERT_ND(!target->is_moved());
  ASSERT_ND(!target->is_retired());
  target->assert_entries();
  *out_page = root;
  return kErrorCodeOk;
}

inline ErrorCode MasstreeStoragePimpl::check_next_layer_bit(xct::XctId observed) {
  if (UNLIKELY(observed.is_next_layer())) {
    // this should have been checked before this method and resolved as abort or retry,
    // but if it reaches here for some reason, we treat it as usual contention abort.
    DLOG(INFO) << "Probably this should be caught beforehand. next_layer bit is on";
    return kErrorCodeXctRaceAbort;
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::retrieve_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  SlotIndex index,
  xct::XctId observed,
  void* payload,
  PayloadLength* payload_capacity) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(observed));
  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    context,
    get_id(),
    observed,
    border->get_owner_id(index),
    border->header().hotness_address()));

  // here, we do NOT have to do another optimistic-read protocol because we already took
  // the owner_id into read-set. If this read is corrupted, we will be aware of it at commit time.
  PayloadLength payload_length = border->get_payload_length(index);
  if (payload_length > *payload_capacity) {
    // buffer too small
    DVLOG(0) << "buffer too small??" << payload_length << ":" << *payload_capacity;
    *payload_capacity = payload_length;
    return kErrorCodeStrTooSmallPayloadBuffer;
  }
  *payload_capacity = payload_length;
  std::memcpy(payload, border->get_record_payload(index), payload_length);
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::retrieve_part_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  SlotIndex index,
  xct::XctId observed,
  void* payload,
  PayloadLength payload_offset,
  PayloadLength  payload_count) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(observed));
  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    context,
    get_id(),
    observed,
    border->get_owner_id(index),
    border->header().hotness_address()));
  if (border->get_payload_length(index) < payload_offset + payload_count) {
    LOG(WARNING) << "short record";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }
  std::memcpy(payload, border->get_record_payload(index) + payload_offset, payload_count);
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::insert_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  SlotIndex index,
  xct::XctId observed,
  const void* be_key,
  KeyLength key_length,
  const void* payload,
  PayloadLength  payload_count) {
  if (!observed.is_deleted()) {
    return kErrorCodeStrKeyAlreadyExists;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(observed));

  // as of reserve_record() it was spacious enough, and this length is
  // either immutable or only increases, so this must hold.
  ASSERT_ND(border->get_max_payload_length(index) >= payload_count);

  uint16_t log_length = MasstreeInsertLogType::calculate_log_length(key_length, payload_count);
  MasstreeInsertLogType* log_entry = reinterpret_cast<MasstreeInsertLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    be_key,
    key_length,
    payload,
    payload_count);
  border->header().stat_last_updater_node_ = context->get_numa_node();

  return context->get_current_xct().add_to_read_and_write_set(
    context,
    get_id(),
    observed,
    border->get_owner_id(index),
    border->get_record(index),
    log_entry,
    border->header().hotness_address());
}

ErrorCode MasstreeStoragePimpl::delete_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  SlotIndex index,
  xct::XctId observed,
  const void* be_key,
  KeyLength key_length) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(observed));

  uint16_t log_length = MasstreeDeleteLogType::calculate_log_length(key_length);
  MasstreeDeleteLogType* log_entry = reinterpret_cast<MasstreeDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), be_key, key_length);
  border->header().stat_last_updater_node_ = context->get_numa_node();

  return context->get_current_xct().add_to_read_and_write_set(
    context,
    get_id(),
    observed,
    border->get_owner_id(index),
    border->get_record(index),
    log_entry,
    border->header().hotness_address());
}

ErrorCode MasstreeStoragePimpl::upsert_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  SlotIndex index,
  xct::XctId observed,
  const void* be_key,
  KeyLength key_length,
  const void* payload,
  PayloadLength  payload_count) {
  // Upsert is a combination of what insert does and what delete does.
  // If there isn't an existing physical record, it's exactly same as insert.
  // If there is, it's _basically_ a delete followed by an insert.
  // There are a few complications, depending on the status of the record.
  CHECK_ERROR_CODE(check_next_layer_bit(observed));

  // as of reserve_record() it was spacious enough, and this length is
  // either immutable or only increases, so this must hold.
  ASSERT_ND(border->get_max_payload_length(index) >= payload_count);

  MasstreeCommonLogType* common_log;
  if (observed.is_deleted()) {
    // If it's a deleted record, this turns to be a plain insert.
    uint16_t log_length = MasstreeInsertLogType::calculate_log_length(key_length, payload_count);
    MasstreeInsertLogType* log_entry = reinterpret_cast<MasstreeInsertLogType*>(
      context->get_thread_log_buffer().reserve_new_log(log_length));
    log_entry->populate(
      get_id(),
      be_key,
      key_length,
      payload,
      payload_count);
    common_log = log_entry;
  } else if (payload_count == border->get_payload_length(index)) {
    // If it's not changing payload size of existing record, we can conver it to an overwrite,
    // which is more efficient
    uint16_t log_length = MasstreeOverwriteLogType::calculate_log_length(key_length, payload_count);
    MasstreeOverwriteLogType* log_entry = reinterpret_cast<MasstreeOverwriteLogType*>(
      context->get_thread_log_buffer().reserve_new_log(log_length));
    log_entry->populate(
      get_id(),
      be_key,
      key_length,
      payload,
      0,
      payload_count);
    common_log = log_entry;
  } else {
    // If not, this is an update operation.
    uint16_t log_length = MasstreeUpdateLogType::calculate_log_length(key_length, payload_count);
    MasstreeUpdateLogType* log_entry = reinterpret_cast<MasstreeUpdateLogType*>(
      context->get_thread_log_buffer().reserve_new_log(log_length));
    log_entry->populate(
      get_id(),
      be_key,
      key_length,
      payload,
      payload_count);
    common_log = log_entry;
  }
  border->header().stat_last_updater_node_ = context->get_numa_node();

  return context->get_current_xct().add_to_read_and_write_set(
    context,
    get_id(),
    observed,
    border->get_owner_id(index),
    border->get_record(index),
    common_log,
    border->header().hotness_address());
}
ErrorCode MasstreeStoragePimpl::overwrite_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  SlotIndex index,
  xct::XctId observed,
  const void* be_key,
  KeyLength key_length,
  const void* payload,
  PayloadLength payload_offset,
  PayloadLength  payload_count) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(observed));
  if (border->get_payload_length(index) < payload_offset + payload_count) {
    LOG(WARNING) << "short record ";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  uint16_t log_length = MasstreeOverwriteLogType::calculate_log_length(key_length, payload_count);
  MasstreeOverwriteLogType* log_entry = reinterpret_cast<MasstreeOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    be_key,
    key_length,
    payload,
    payload_offset,
    payload_count);
  border->header().stat_last_updater_node_ = context->get_numa_node();

  return context->get_current_xct().add_to_read_and_write_set(
    context,
    get_id(),
    observed,
    border->get_owner_id(index),
    border->get_record(index),
    log_entry,
    border->header().hotness_address());
}

template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::increment_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  SlotIndex index,
  xct::XctId observed,
  const void* be_key,
  KeyLength key_length,
  PAYLOAD* value,
  PayloadLength payload_offset) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(observed));
  if (border->get_payload_length(index) < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record ";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  char* ptr = border->get_record_payload(index) + payload_offset;
  *value += *reinterpret_cast<const PAYLOAD*>(ptr);

  uint16_t log_length = MasstreeOverwriteLogType::calculate_log_length(key_length, sizeof(PAYLOAD));
  MasstreeOverwriteLogType* log_entry = reinterpret_cast<MasstreeOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    be_key,
    key_length,
    value,
    payload_offset,
    sizeof(PAYLOAD));
  border->header().stat_last_updater_node_ = context->get_numa_node();

  return context->get_current_xct().add_to_read_and_write_set(
    context,
    get_id(),
    observed,
    border->get_owner_id(index),
    border->get_record(index),
    log_entry,
    border->header().hotness_address());
}

inline xct::TrackMovedRecordResult MasstreeStoragePimpl::track_moved_record(
  xct::RwLockableXctId* old_address,
  xct::WriteXctAccess* write_set) {
  ASSERT_ND(old_address);
  // We use moved bit only for volatile border pages
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(to_page(old_address));
  ASSERT_ND(page->is_border());
  ASSERT_ND(page->is_moved() || old_address->is_next_layer());
  return page->track_moved_record(engine_, old_address, write_set);
}

// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_5(x) template ErrorCode MasstreeStoragePimpl::increment_general< x > \
  (thread::Thread* context, MasstreeBorderPage* border, SlotIndex index, xct::XctId observed, \
  const void* be_key, KeyLength key_length, x* value, PayloadLength payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);
// @endcond

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
