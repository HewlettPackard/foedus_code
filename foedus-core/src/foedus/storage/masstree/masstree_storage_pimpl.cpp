/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
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
bool MasstreeStorage::track_moved_record(xct::WriteXctAccess* write) {
  return MasstreeStoragePimpl(this).track_moved_record(write);
}
xct::LockableXctId* MasstreeStorage::track_moved_record(xct::LockableXctId* address) {
  return MasstreeStoragePimpl(this).track_moved_record(address);
}
ErrorStack MasstreeStoragePimpl::drop() {
  LOG(INFO) << "Uninitializing a masstree-storage " << get_name();

  if (control_block_->root_page_pointer_.volatile_pointer_.components.offset) {
    // release volatile pages
    const memory::GlobalVolatilePageResolver& page_resolver
      = engine_->get_memory_manager()->get_global_volatile_page_resolver();
    MasstreePage* first_root = reinterpret_cast<MasstreePage*>(
      page_resolver.resolve_offset(control_block_->root_page_pointer_.volatile_pointer_));
    memory::PageReleaseBatch release_batch(engine_);
    first_root->release_pages_recursive_common(page_resolver, &release_batch);
    release_batch.release_all();
    control_block_->root_page_pointer_.volatile_pointer_.word = 0;
  }

  return kRetOk;
}

ErrorCode MasstreeStoragePimpl::get_first_root(
  thread::Thread* context,
  MasstreeIntermediatePage** root) {
  ASSERT_ND(get_first_root_pointer().volatile_pointer_.components.offset);
  const memory::GlobalVolatilePageResolver& resolver = context->get_global_volatile_page_resolver();
  MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(
    resolver.resolve_offset(control_block_->root_page_pointer_.volatile_pointer_));
  assert_aligned_page(page);

  if (UNLIKELY(page->has_foster_child())) {
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
  xct::LockableXctId* root_pointer_owner,
  MasstreeIntermediatePage** new_root) {
  *new_root = nullptr;
  if (root_pointer_owner->is_keylocked()) {
    DVLOG(0) << "interesting. someone else is growing the tree, so let him do that.";
    // we can move on, thanks to the master-tree invariant. tree-growth is not a mandatory
    // task to do right away
    return kErrorCodeOk;
  }

  xct::McsLockScope owner_scope(context, root_pointer_owner);
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
  root_pointer->snapshot_pointer_ = 0;
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
            minipage_index,
            pointer_index,
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
  uint16_t key_length,
  bool for_writes,
  MasstreeBorderPage** out_page,
  uint8_t* record_index,
  xct::XctId* observed) {
  ASSERT_ND(key_length <= kMaxKeyLength);
  MasstreePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(
    context,
    reinterpret_cast<MasstreeIntermediatePage**>(&layer_root)));
  for (uint16_t current_layer = 0;; ++current_layer) {
    uint8_t remaining_length = key_length - current_layer * 8;
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
    uint8_t index = border->find_key(slice, suffix, remaining_length);

    if (index == MasstreeBorderPage::kMaxKeys) {
      // this means not found. add it to page version set to protect the lack of record
      if (!border->header().snapshot_) {
        CHECK_ERROR_CODE(context->get_current_xct().add_to_page_version_set(
          border->get_version_address(),
          border_version));
      }
      // TODO(Hideaki) range lock rather than page-set to improve concurrency?
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
  uint8_t* record_index,
  xct::XctId* observed) {
  MasstreeBorderPage* border;

  MasstreeIntermediatePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(context, &layer_root));
  CHECK_ERROR_CODE(find_border(context, layer_root, 0, for_writes, key, &border));
  uint8_t index = border->find_key_normalized(0, border->get_key_count(), key);
  PageVersionStatus border_version = border->get_version().status_;
  if (index == MasstreeBorderPage::kMaxKeys) {
    // this means not found
    if (!border->header().snapshot_) {
      CHECK_ERROR_CODE(context->get_current_xct().add_to_page_version_set(
        border->get_version_address(),
        border_version));
    }
    // TODO(Hideaki) range lock
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
  uint8_t parent_index) {
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

  xct::LockableXctId* parent_lock = parent->get_owner_id(parent_index);

  // as an independent system transaction, here we do an optimistic version check.
  xct::McsLockScope parent_scope(context, parent_lock);
  if (parent_lock->is_moved() || parent->does_point_to_layer(parent_index)) {
    // someone else has also made this to a next layer or the page itself is moved!
    // our effort was a waste, but anyway the goal was achieved.
    VLOG(0) << "interesting. a concurrent thread has already made "
      << (parent_lock->is_moved() ? "this page moved" : " it point to next layer");
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

    // point to the new page
    parent_lock->xct_id_.set_being_written();
    assorted::memory_fence_release();
    parent->set_next_layer(parent_index, pointer);
    ASSERT_ND(parent->get_next_layer(parent_index)->volatile_pointer_.components.offset == offset);
    ASSERT_ND(parent->get_next_layer(parent_index)->volatile_pointer_.components.numa_node
      == context->get_numa_node());

    if (parent_lock->is_deleted()) {
      // if the original record was deleted, we inherited it in the new record too.
      // again, we didn't do anything logically.
      ASSERT_ND(root->get_owner_id(0)->is_deleted());
      // as a pointer, now it should be an active pointer.
      parent_lock->xct_id_.set_notdeleted();
    }
    // change ordinal just to let concurrent transactions get aware
    parent_lock->xct_id_.set_ordinal(parent_lock->xct_id_.get_ordinal() + 1U);
    assorted::memory_fence_release();
    parent_lock->xct_id_.set_write_complete();
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::follow_page(
  thread::Thread* context,
  bool for_writes,
  storage::DualPagePointer* pointer,
  MasstreePage** page) {
  return context->follow_page_pointer(
    &kDummyPageInitializer,  // masstree doesn't create a new page except splits.
    false,  // so, there is no null page possible
    for_writes,  // always get volatile pages for writes
    true,
    false,  // root pointer might change, but we have is_root check. so no need for pointer set
    pointer,
    reinterpret_cast<Page**>(page));
}

inline ErrorCode MasstreeStoragePimpl::follow_layer(
  thread::Thread* context,
  bool for_writes,
  MasstreeBorderPage* parent,
  uint8_t record_index,
  MasstreePage** page) {
  ASSERT_ND(record_index < MasstreeBorderPage::kMaxKeys);
  ASSERT_ND(parent->does_point_to_layer(record_index));
  DualPagePointer* pointer = parent->get_next_layer(record_index);
  xct::LockableXctId* owner = parent->get_owner_id(record_index);
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

ErrorCode MasstreeStoragePimpl::reserve_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  uint16_t payload_count,
  MasstreeBorderPage** out_page,
  uint8_t* record_index,
  xct::XctId* observed) {
  ASSERT_ND(key_length <= kMaxKeyLength);

  MasstreePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(
    context,
    reinterpret_cast<MasstreeIntermediatePage**>(&layer_root)));
  for (uint16_t layer = 0;; ++layer) {
    const uint8_t remaining = key_length - layer * sizeof(KeySlice);
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

      uint8_t count = border->get_key_count();
      // as done in reserve_record_new_record_apply(), we need a fence on BOTH sides.
      // observe key count first, then verify the keys.
      assorted::memory_fence_consume();
      MasstreeBorderPage::FindKeyForReserveResult match = border->find_key_for_reserve(
        0,
        count,
        slice,
        suffix,
        remaining);

      if (match.match_type_ == MasstreeBorderPage::kExactMatchLayerPointer) {
        ASSERT_ND(match.index_ < MasstreeBorderPage::kMaxKeys);
        CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
        break;  // next layer
      } else if (match.match_type_ == MasstreeBorderPage::kExactMatchLocalRecord) {
        // TODO(Hideaki) even if in this case, if the record space is too small, we can't insert.
        // in that case, we should do delete then insert.
        *out_page = border;
        *record_index = match.index_;
        *observed = border->get_owner_id(match.index_)->xct_id_;
        assorted::memory_fence_consume();
        return kErrorCodeOk;
      } else if (match.match_type_ == MasstreeBorderPage::kConflictingLocalRecord) {
        // in this case, we don't need a page-wide lock. because of key-immutability,
        // this is the only place we can have a next-layer pointer for this slice.
        // thus we just lock the record and convert it to a next-layer pointer.
        ASSERT_ND(match.index_ < MasstreeBorderPage::kMaxKeys);
        // this means now we have to create a next layer.
        // this is also one system transaction.
        CHECK_ERROR_CODE(create_next_layer(context, border, match.index_));
        // because we do this without page lock, this might have failed. in that case,
        // we retry.
        if (border->does_point_to_layer(match.index_)) {
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
        uint8_t new_count = border->get_key_count();
        match = border->find_key_for_reserve(count, new_count, slice, suffix, remaining);
        count = new_count;
      }

      if (match.match_type_ == MasstreeBorderPage::kExactMatchLayerPointer) {
        scope.release();
        CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
        break;  // next layer
      } else if (match.match_type_ == MasstreeBorderPage::kExactMatchLocalRecord) {
        scope.release();
        *out_page = border;
        *record_index = match.index_;
        *observed = border->get_owner_id(match.index_)->xct_id_;
        assorted::memory_fence_consume();
        return kErrorCodeOk;
      } else if (match.match_type_ == MasstreeBorderPage::kNotFound) {
        // okay, surely new record
        scope.set_changed();
        ErrorCode code = reserve_record_new_record(
          context,
          border,
          slice,
          remaining,
          suffix,
          payload_count,
          out_page,
          record_index,
          observed);
        ASSERT_ND(!(*out_page)->is_moved());
        ASSERT_ND(!(*out_page)->is_retired());
        ASSERT_ND(*record_index < (*out_page)->get_key_count());
        return code;
      } else {
        ASSERT_ND(match.match_type_ == MasstreeBorderPage::kConflictingLocalRecord);
        ASSERT_ND(match.index_ < MasstreeBorderPage::kMaxKeys);
        // this means now we have to create a next layer.
        // this is also one system transaction.
        scope.set_changed();
        CHECK_ERROR_CODE(create_next_layer(context, border, match.index_));
        border->assert_entries();
        // because of page lock, this always succeeds (unlike the above w/o page lock)
        ASSERT_ND(border->does_point_to_layer(match.index_));
        CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
        ASSERT_ND(!border->is_moved());
        ASSERT_ND(!border->is_retired());
        break;  // next layer
      }
    }
  }
}

ErrorCode MasstreeStoragePimpl::reserve_record_normalized(
  thread::Thread* context,
  KeySlice key,
  uint16_t payload_count,
  MasstreeBorderPage** out_page,
  uint8_t* record_index,
  xct::XctId* observed) {
  const uint8_t kRemaining = sizeof(KeySlice);
  MasstreeBorderPage* border;

  MasstreeIntermediatePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(context, &layer_root));
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
    uint8_t count = border->get_key_count();
    uint8_t index = border->find_key_normalized(0, count, key);

    if (index != MasstreeBorderPage::kMaxKeys) {
      // TODO(Hideaki) even if in this case, if the record space is too small, we can't insert.
      // in that case, we should do delete then insert.
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
      kRemaining,
      nullptr,
      payload_count,
      out_page,
      record_index,
      observed);
  }
}

ErrorCode MasstreeStoragePimpl::reserve_record_new_record(
  thread::Thread* context,
  MasstreeBorderPage* border,
  KeySlice key,
  uint8_t remaining,
  const void* suffix,
  uint16_t payload_count,
  MasstreeBorderPage** out_page,
  uint8_t* record_index,
  xct::XctId* observed) {
  ASSERT_ND(border->is_locked());
  ASSERT_ND(!border->is_moved());
  ASSERT_ND(border->get_foster_major().is_null());
  ASSERT_ND(border->get_foster_minor().is_null());
  uint8_t count = border->get_key_count();
  if (!border->should_split_early(count, get_meta().border_early_split_threshold_) &&
    border->can_accomodate(count, remaining, payload_count)) {
    reserve_record_new_record_apply(
      context,
      border,
      count,
      key,
      remaining,
      suffix,
      payload_count,
      observed);
    *out_page = border;
    *record_index = count;
  } else {
#ifndef NDEBUG
    if (border->should_split_early(count, get_meta().border_early_split_threshold_)) {
      LOG(INFO) << "Early split! cur count=" << static_cast<int>(count)
        << ", storage=" << get_name();
    }
#endif  // NDEBUG
    // have to split to make room. the newly created foster child is always the place to insert.
    MasstreeBorderPage* target;
    xct::McsBlockIndex target_lock;
    CHECK_ERROR_CODE(border->split_foster(context, key, &target, &target_lock));
    ASSERT_ND(target->is_locked());
    ASSERT_ND(target->within_fences(key));
    count = target->get_key_count();
    ASSERT_ND(target->find_key(key, suffix, remaining) == MasstreeBorderPage::kMaxKeys);
    if (!target->can_accomodate(count, remaining, payload_count)) {
      // this might happen if payload_count is huge. so far just error out.
      LOG(WARNING) << "Wait, not enough space even after splits? should be pretty rare...";
      context->mcs_release_lock(target->get_lock_address(), target_lock);
      return kErrorCodeStrTooLongPayload;
    }
    target->get_version_address()->increment_version_counter();
    reserve_record_new_record_apply(
      context,
      target,
      count,
      key,
      remaining,
      suffix,
      payload_count,
      observed);
    context->mcs_release_lock(target->get_lock_address(), target_lock);
    *out_page = target;
    *record_index = count;
  }
  return kErrorCodeOk;
}

void MasstreeStoragePimpl::reserve_record_new_record_apply(
  thread::Thread* /*context*/,
  MasstreeBorderPage* target,
  uint8_t target_index,
  KeySlice slice,
  uint8_t remaining_key_length,
  const void* suffix,
  uint16_t payload_count,
  xct::XctId* observed) {
  ASSERT_ND(target->is_locked());
  ASSERT_ND(!target->is_moved());
  ASSERT_ND(!target->is_retired());
  ASSERT_ND(target->can_accomodate(target_index, remaining_key_length, payload_count));
  ASSERT_ND(target->get_key_count() < MasstreeBorderPage::kMaxKeys);
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
    remaining_key_length,
    payload_count);
  // we increment key count AFTER installing the key because otherwise the optimistic read
  // might see the record but find that the key doesn't match. we need a fence to prevent it.
  assorted::memory_fence_release();
  target->increment_key_count();
  ASSERT_ND(target->get_key_count() <= MasstreeBorderPage::kMaxKeys);
  ASSERT_ND(!target->is_moved());
  ASSERT_ND(!target->is_retired());
  target->assert_entries();
}

ErrorCode MasstreeStoragePimpl::retrieve_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  xct::XctId observed,
  void* payload,
  uint16_t* payload_capacity) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  // TODO(Hideaki) does_point_to_layer should be a flag in XctId
  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    get_id(),
    observed,
    border->get_owner_id(index)));

  // here, we do NOT have to do another optimistic-read protocol because we already took
  // the owner_id into read-set. If this read is corrupted, we will be aware of it at commit time.
  uint16_t payload_length = border->get_payload_length(index);
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
  uint8_t index,
  xct::XctId observed,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  // TODO(Hideaki) does_point_to_layer should be a flag in XctId
  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    get_id(),
    observed,
    border->get_owner_id(index)));
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
  uint8_t index,
  xct::XctId observed,
  const void* be_key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  if (!observed.is_deleted()) {
    return kErrorCodeStrKeyAlreadyExists;
  }
  // TODO(Hideaki) does_point_to_layer should be a flag in XctId

  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    get_id(),
    observed,
    border->get_owner_id(index)));

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

  return context->get_current_xct().add_to_write_set(
    get_id(),
    border->get_owner_id(index),
    border->get_record(index),
    log_entry);
}

ErrorCode MasstreeStoragePimpl::delete_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  xct::XctId observed,
  const void* be_key,
  uint16_t key_length) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  // TODO(Hideaki) does_point_to_layer should be a flag in XctId
  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    get_id(),
    observed,
    border->get_owner_id(index)));

  uint16_t log_length = MasstreeDeleteLogType::calculate_log_length(key_length);
  MasstreeDeleteLogType* log_entry = reinterpret_cast<MasstreeDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), be_key, key_length);
  border->header().stat_last_updater_node_ = context->get_numa_node();

  return context->get_current_xct().add_to_write_set(
    get_id(),
    border->get_owner_id(index),
    border->get_record(index),
    log_entry);
}

ErrorCode MasstreeStoragePimpl::overwrite_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  xct::XctId observed,
  const void* be_key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  // TODO(Hideaki) does_point_to_layer should be a flag in XctId
  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    get_id(),
    observed,
    border->get_owner_id(index)));

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

  return context->get_current_xct().add_to_write_set(
    get_id(),
    border->get_owner_id(index),
    border->get_record(index),
    log_entry);
}

template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::increment_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  xct::XctId observed,
  const void* be_key,
  uint16_t key_length,
  PAYLOAD* value,
  uint16_t payload_offset) {
  if (observed.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  // TODO(Hideaki) does_point_to_layer should be a flag in XctId
  CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(
    get_id(),
    observed,
    border->get_owner_id(index)));

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

  return context->get_current_xct().add_to_write_set(
    get_id(),
    border->get_owner_id(index),
    border->get_record(index),
    log_entry);
}

inline bool MasstreeStoragePimpl::track_moved_record(xct::WriteXctAccess* write) {
  // We use moved bit only for volatile border pages
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(
    to_page(write->owner_id_address_));
  ASSERT_ND(page == reinterpret_cast<MasstreeBorderPage*>(to_page(write->payload_address_)));
  MasstreeBorderPage* located_page;
  uint8_t located_index;
  if (!page->track_moved_record(engine_, write->owner_id_address_, &located_page, &located_index)) {
    return false;
  }
  write->owner_id_address_ = located_page->get_owner_id(located_index);
  write->payload_address_ = located_page->get_record(located_index);
  return true;
}

inline xct::LockableXctId* MasstreeStoragePimpl::track_moved_record(xct::LockableXctId* address) {
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(to_page(address));
  MasstreeBorderPage* located_page;
  uint8_t located_index;
  if (!page->track_moved_record(engine_, address, &located_page, &located_index)) {
    return nullptr;
  }
  return located_page->get_owner_id(located_index);
}

// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_5(x) template ErrorCode MasstreeStoragePimpl::increment_general< x > \
  (thread::Thread* context, MasstreeBorderPage* border, uint8_t index, xct::XctId observed, \
  const void* be_key, uint16_t key_length, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);
// @endcond

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
