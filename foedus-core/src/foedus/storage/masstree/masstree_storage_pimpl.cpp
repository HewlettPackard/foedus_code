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
#include "foedus/storage/masstree/masstree_adopt_impl.hpp"
#include "foedus/storage/masstree/masstree_grow_impl.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_record_location.hpp"
#include "foedus/storage/masstree/masstree_reserve_impl.hpp"
#include "foedus/storage/masstree/masstree_split_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/////////////////////////////////////////////////////////////////////////////
///
///  Root-node related, such as a method to retrieve 1st-root, to grow, etc.
///
/////////////////////////////////////////////////////////////////////////////
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
  ASSERT_ND(page->get_layer() == 0);
  ASSERT_ND(page->get_low_fence() == kInfimumSlice);
  ASSERT_ND(page->get_high_fence() == kSupremumSlice);
  ASSERT_ND(!for_write || !page->header().snapshot_);

  if (UNLIKELY(page->has_foster_child())) {
    ASSERT_ND(!page->header().snapshot_);
    // root page has a foster child... time for tree growth!
    // either case, we just follow the moved page. Master-Tree invariant guarantees it's safe.
    if (control_block_->first_root_locked_) {
      // To avoid contention, we begin it only when the lock seems uncontended.
      DVLOG(0) << "Other thread seems growing the first-layer. let him do that";
    } else {
      GrowFirstLayerRoot functor(context, get_id());
      CHECK_ERROR_CODE(context->run_nested_sysxct(&functor, 2));
    }
  }

  ASSERT_ND(page->get_layer() == 0);
  ASSERT_ND(page->get_low_fence() == kInfimumSlice);
  ASSERT_ND(page->get_high_fence() == kSupremumSlice);
  *root = page;
  return kErrorCodeOk;
}

/////////////////////////////////////////////////////////////////////////////
///
///  Storage-wide operations, such as drop, create, etc
///
/////////////////////////////////////////////////////////////////////////////
ErrorStack MasstreeStoragePimpl::drop() {
  LOG(INFO) << "Uninitializing a masstree-storage " << get_name();

  if (!control_block_->root_page_pointer_.volatile_pointer_.is_null()) {
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
  control_block_->first_root_locked_ = false;
  control_block_->root_page_pointer_.snapshot_pointer_ = 0;
  control_block_->root_page_pointer_.volatile_pointer_.set(kDummyNode, root_offset);
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
  VolatilePagePointer child_pointer;
  child_pointer.set(kDummyNode, child_offset);
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
  control_block_->first_root_locked_ = false;

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
    control_block_->root_page_pointer_.volatile_pointer_ = volatile_pointer;
  } else {
    LOG(INFO) << "This is an empty masstree: " << get_meta();
    CHECK_ERROR(load_empty());
  }

  control_block_->status_ = kExists;
  LOG(INFO) << "Loaded a masstree-storage " << get_meta();
  return kRetOk;
}

/////////////////////////////////////////////////////////////////////////////
///
///  Record-wise or page-wise operations
///
/////////////////////////////////////////////////////////////////////////////
inline ErrorCode MasstreeStoragePimpl::find_border_physical(
  thread::Thread* context,
  MasstreePage* layer_root,
  uint8_t   current_layer,
  bool      for_writes,
  KeySlice  slice,
  MasstreeBorderPage** border) {
#ifndef NDEBUG
retry_from_layer_root:
  uint16_t retried_count = 0;
  constexpr uint16_t kMaxRetryCount = 100;
#endif  // NDEBUG
  assert_aligned_page(layer_root);
  ASSERT_ND(layer_root->is_high_fence_supremum());
  ASSERT_ND(layer_root->get_low_fence() == kInfimumSlice);
  MasstreePage* cur = layer_root;
  cur->prefetch_general();
  while (true) {
    assert_aligned_page(cur);
    ASSERT_ND(cur->get_layer() == current_layer);
    ASSERT_ND(cur->within_fences(slice));
    if (cur->is_border()) {
      // We follow foster-twins only in border pages.
      // In intermediate pages, Master-Tree invariant tells us that we don't have to.
      // Furthermore, if we do, we need to handle the case of empty-range intermediate pages.
      // Rather we just do this only in border pages.
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
      if (LIKELY(next->within_fences(slice))) {
#ifndef NDEBUG
        retried_count = 0;
#endif  // NDEBUG
        if (next->has_foster_child() && !cur->is_moved()) {
          // oh, the page has foster child, so we should adopt it.
          // Whether Adopt actually adopted it or not,
          // we follow the "old" next page. Master-Tree invariant guarantees that it's safe.
          // This is beneficial when we lazily give up adoption in the method, eg other threads
          // holding locks in the intermediate page.
          if (!next->is_locked() && !cur->is_locked()) {
            // Let's try adopting. No need to try many times. Adopt can be delayed
            Adopt functor(context, page, next);
            CHECK_ERROR_CODE(context->run_nested_sysxct(&functor, 2));
          } else {
            // We don't have to adopt it right away. Do it when it's not contended
            DVLOG(1) << "Someone else seems doing something there.. already adopting? skip it";
          }
        }
        cur = next;
      } else {
        // even in this case, local retry suffices thanks to foster-twin
        DVLOG(0) << "Interesting. concurrent thread affected the search. local retry";
        assorted::memory_fence_acquire();
#ifndef NDEBUG
        ++retried_count;
        if (retried_count > kMaxRetryCount) {
          LOG(ERROR) << "WTF?? Too frequent retries. Are we really in the right page?"
            " This is most likely a bug.";
          goto retry_from_layer_root;
        }
#endif  // NDEBUG
      }
    }
  }
}

ErrorCode MasstreeStoragePimpl::locate_record(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  bool for_writes,
  RecordLocation* result) {
  ASSERT_ND(result);
  ASSERT_ND(key_length <= kMaxKeyLength);
  result->clear();
  xct::Xct* cur_xct = &context->get_current_xct();
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
    CHECK_ERROR_CODE(find_border_physical(
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
        CHECK_ERROR_CODE(cur_xct->add_to_page_version_set(
          border->get_version_address(),
          border_version));
      }
      // So far we just use page-version set to protect this non-existence.
      // TASK(Hideaki) range lock rather than page-set to improve concurrency?
      result->clear();
      return kErrorCodeStrKeyNotFound;
    }
    if (border->does_point_to_layer(index)) {
      CHECK_ERROR_CODE(follow_layer(context, for_writes, border, index, &layer_root));
      continue;
    } else {
      CHECK_ERROR_CODE(result->populate_logical(cur_xct, border, index, for_writes));
      return kErrorCodeOk;
    }
  }
}

ErrorCode MasstreeStoragePimpl::locate_record_normalized(
  thread::Thread* context,
  KeySlice key,
  bool for_writes,
  RecordLocation* result) {
  ASSERT_ND(result);
  result->clear();
  xct::Xct* cur_xct = &context->get_current_xct();
  MasstreeBorderPage* border;
  MasstreeIntermediatePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(context, for_writes, &layer_root));
  CHECK_ERROR_CODE(find_border_physical(context, layer_root, 0, for_writes, key, &border));
  SlotIndex index = border->find_key_normalized(0, border->get_key_count(), key);
  PageVersionStatus border_version = border->get_version().status_;
  if (index == kBorderPageMaxSlots) {
    // this means not found
    if (!border->header().snapshot_) {
      CHECK_ERROR_CODE(cur_xct->add_to_page_version_set(
        border->get_version_address(),
        border_version));
    }
    // TASK(Hideaki) range lock
    result->clear();
    return kErrorCodeStrKeyNotFound;
  }
  // because this is just one slice, we never go to second layer
  ASSERT_ND(!border->does_point_to_layer(index));
  CHECK_ERROR_CODE(result->populate_logical(cur_xct, border, index, for_writes));
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::follow_page(
  thread::Thread* context,
  bool for_writes,
  storage::DualPagePointer* pointer,
  MasstreePage** page) {
  ASSERT_ND(!pointer->is_both_null());
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
    ASSERT_ND(next_root->get_layer() > 0);
    // Either case, we follow the old page. Master-Tree invariant guarantees it's safe
    GrowNonFirstLayerRoot functor(context, parent, record_index);
    CHECK_ERROR_CODE(context->run_nested_sysxct(&functor, 2U));
  }

  ASSERT_ND(next_root);
  *page = next_root;
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::reserve_record(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  PayloadLength payload_count,
  PayloadLength physical_payload_hint,
  RecordLocation* result) {
  ASSERT_ND(key_length <= kMaxKeyLength);
  ASSERT_ND(physical_payload_hint >= payload_count);
  ASSERT_ND(result);
  result->clear();
  xct::Xct* cur_xct = &context->get_current_xct();

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
    CHECK_ERROR_CODE(find_border_physical(context, layer_root, layer, true, slice, &border));
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

      const SlotIndex count = border->get_key_count();
      // ReserveRecords, we need a fence on BOTH sides.
      // observe key count first, then verify the keys.
      assorted::memory_fence_acquire();
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
        if (border->get_max_payload_length(match.index_) >= payload_count) {
          // Ok, this seems to already satisfy our need. but...
          // Up until now, all the searches/expands were physical-only.
          // Now we finalize the XID in the search result so that precommit will
          // catch any change since now. This also means we need to re-check
          // the status again, and retry if pointing to next layer or moved.
          CHECK_ERROR_CODE(result->populate_logical(cur_xct, border, match.index_, true));
          if (result->observed_.is_next_layer() || result->observed_.is_moved()) {
            // because the search is optimistic, we might now see a XctId with next-layer bit on.
            // in this case, we retry.
            VLOG(0) << "Interesting. Next-layer-retry due to concurrent transaction";
            assorted::memory_fence_acquire();
            continue;
          }
          return kErrorCodeOk;
        }
      }

      // In all other cases, we (probably) need to do something complex.
      // ReserveRecords sysxct does it.
      ReserveRecords reserve(
        context,
        border,
        slice,
        remainder,
        suffix,
        physical_payload_hint,  // let's allocate conservatively
        get_meta().should_aggresively_create_next_layer(layer, remainder),
        match.match_type_ == MasstreeBorderPage::kNotFound ? count : match.index_);
      CHECK_ERROR_CODE(context->run_nested_sysxct(&reserve, 2U));

      // We might need to split the page
      if (reserve.out_split_needed_) {
        SplitBorder split(
          context,
          border,
          slice,
          false,
          true,
          remainder,
          physical_payload_hint,
          suffix);
        CHECK_ERROR_CODE(context->run_nested_sysxct(&split, 2U));
      }

      // In either case, we should resume the search.
      continue;
    }
  }
}

ErrorCode MasstreeStoragePimpl::reserve_record_normalized(
  thread::Thread* context,
  KeySlice key,
  PayloadLength payload_count,
  PayloadLength physical_payload_hint,
  RecordLocation* result) {
  ASSERT_ND(result);
  result->clear();
  xct::Xct* cur_xct = &context->get_current_xct();
  MasstreeBorderPage* border;

  MasstreeIntermediatePage* layer_root;
  CHECK_ERROR_CODE(get_first_root(context, true, &layer_root));
  CHECK_ERROR_CODE(find_border_physical(context, layer_root, 0, true, key, &border));
  while (true) {  // retry loop for following foster child
    // if we found out that the page was split and we should follow foster child, do it.
    while (border->has_foster_child()) {
      if (border->within_foster_minor(key)) {
        border = context->resolve_cast<MasstreeBorderPage>(border->get_foster_minor());
      } else {
        border = context->resolve_cast<MasstreeBorderPage>(border->get_foster_major());
      }
    }

    ASSERT_ND(border->within_fences(key));

    // because we never go on to second layer in this case, it's either a full match or not-found
    const SlotIndex count = border->get_key_count();
    assorted::memory_fence_acquire();
    const SlotIndex index = border->find_key_normalized(0, count, key);

    if (index != kBorderPageMaxSlots) {
      // If the record space is too small, we can't insert.
      if (border->get_max_payload_length(index) >= payload_count) {
        CHECK_ERROR_CODE(result->populate_logical(cur_xct, border, index, true));
        if (result->observed_.is_moved()) {
          // because the search is optimistic, we might now see a XctId with next-layer bit on.
          // in this case, we retry.
          VLOG(0) << "Interesting. Moved by concurrent transaction";
          assorted::memory_fence_acquire();
          continue;
        }
        return kErrorCodeOk;
      }
    }

    // Same as the general case.
    ReserveRecords reserve(
      context,
      border,
      key,
      sizeof(KeySlice),
      nullptr,
      physical_payload_hint,
      false,
      index == kBorderPageMaxSlots ? count : index);
    CHECK_ERROR_CODE(context->run_nested_sysxct(&reserve, 2U));

    if (reserve.out_split_needed_) {
      SplitBorder split(
        context,
        border,
        key,
        false,
        true,
        sizeof(KeySlice),
        physical_payload_hint,
        nullptr);
      CHECK_ERROR_CODE(context->run_nested_sysxct(&split, 2U));
    }
    continue;
  }
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
  thread::Thread* /*context*/,
  const RecordLocation& location,
  void* payload,
  PayloadLength* payload_capacity) {
  if (location.observed_.is_deleted()) {
    // This result is protected by readset
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(location.observed_));

  // here, we do NOT have to do another optimistic-read protocol because we already took
  // the owner_id into read-set. If this read is corrupted, we will be aware of it at commit time.
  MasstreeBorderPage* border = location.page_;
  PayloadLength payload_length = border->get_payload_length(location.index_);
  if (payload_length > *payload_capacity) {
    // buffer too small
    DVLOG(0) << "buffer too small??" << payload_length << ":" << *payload_capacity;
    *payload_capacity = payload_length;
    return kErrorCodeStrTooSmallPayloadBuffer;
  }
  *payload_capacity = payload_length;
  std::memcpy(payload, border->get_record_payload(location.index_), payload_length);
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::retrieve_part_general(
  thread::Thread* /*context*/,
  const RecordLocation& location,
  void* payload,
  PayloadLength payload_offset,
  PayloadLength  payload_count) {
  if (location.observed_.is_deleted()) {
    // This result is protected by readset
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(location.observed_));
  MasstreeBorderPage* border = location.page_;
  if (border->get_payload_length(location.index_) < payload_offset + payload_count) {
    LOG(WARNING) << "short record";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }
  std::memcpy(payload, border->get_record_payload(location.index_) + payload_offset, payload_count);
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::register_record_write_log(
  thread::Thread* context,
  const RecordLocation& location,
  log::RecordLogType* log_entry) {
  // If we have taken readset in locate_record, add as a related write set
  MasstreeBorderPage* border = location.page_;
  auto* tid = border->get_owner_id(location.index_);
  char* record = border->get_record(location.index_);
  xct::Xct* cur_xct = &context->get_current_xct();
  if (location.readset_) {
    return cur_xct->add_related_write_set(location.readset_, tid, record, log_entry);
  } else {
    return cur_xct->add_to_write_set(get_id(), tid, record, log_entry);
  }
}

ErrorCode MasstreeStoragePimpl::insert_general(
  thread::Thread* context,
  const RecordLocation& location,
  const void* be_key,
  KeyLength key_length,
  const void* payload,
  PayloadLength  payload_count) {
  if (!location.observed_.is_deleted()) {
    return kErrorCodeStrKeyAlreadyExists;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(location.observed_));

  // as of reserve_record() it was spacious enough, and this length is
  // either immutable or only increases, so this must hold.
  MasstreeBorderPage* border = location.page_;
  ASSERT_ND(border->get_max_payload_length(location.index_) >= payload_count);

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
  return register_record_write_log(context, location, log_entry);
}

ErrorCode MasstreeStoragePimpl::delete_general(
  thread::Thread* context,
  const RecordLocation& location,
  const void* be_key,
  KeyLength key_length) {
  if (location.observed_.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(location.observed_));

  MasstreeBorderPage* border = location.page_;
  uint16_t log_length = MasstreeDeleteLogType::calculate_log_length(key_length);
  MasstreeDeleteLogType* log_entry = reinterpret_cast<MasstreeDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), be_key, key_length);
  border->header().stat_last_updater_node_ = context->get_numa_node();
  return register_record_write_log(context, location, log_entry);
}

ErrorCode MasstreeStoragePimpl::upsert_general(
  thread::Thread* context,
  const RecordLocation& location,
  const void* be_key,
  KeyLength key_length,
  const void* payload,
  PayloadLength  payload_count) {
  // Upsert is a combination of what insert does and what delete does.
  // If there isn't an existing physical record, it's exactly same as insert.
  // If there is, it's _basically_ a delete followed by an insert.
  // There are a few complications, depending on the status of the record.
  CHECK_ERROR_CODE(check_next_layer_bit(location.observed_));

  // as of reserve_record() it was spacious enough, and this length is
  // either immutable or only increases, so this must hold.
  MasstreeBorderPage* border = location.page_;
  ASSERT_ND(border->get_max_payload_length(location.index_) >= payload_count);

  MasstreeCommonLogType* common_log;
  if (location.observed_.is_deleted()) {
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
  } else if (payload_count == border->get_payload_length(location.index_)) {
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
  return register_record_write_log(context, location, common_log);
}
ErrorCode MasstreeStoragePimpl::overwrite_general(
  thread::Thread* context,
  const RecordLocation& location,
  const void* be_key,
  KeyLength key_length,
  const void* payload,
  PayloadLength payload_offset,
  PayloadLength  payload_count) {
  if (location.observed_.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(location.observed_));
  MasstreeBorderPage* border = location.page_;
  if (border->get_payload_length(location.index_) < payload_offset + payload_count) {
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
  return register_record_write_log(context, location, log_entry);
}

template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::increment_general(
  thread::Thread* context,
  const RecordLocation& location,
  const void* be_key,
  KeyLength key_length,
  PAYLOAD* value,
  PayloadLength payload_offset) {
  if (location.observed_.is_deleted()) {
    // in this case, we don't need a page-version set. the physical record is surely there.
    return kErrorCodeStrKeyNotFound;
  }
  CHECK_ERROR_CODE(check_next_layer_bit(location.observed_));
  MasstreeBorderPage* border = location.page_;
  if (border->get_payload_length(location.index_) < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record ";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  char* ptr = border->get_record_payload(location.index_) + payload_offset;
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
  return register_record_write_log(context, location, log_entry);
}

// Defines MasstreeStorage methods so that we can inline implementation calls
xct::TrackMovedRecordResult MasstreeStorage::track_moved_record(
  xct::RwLockableXctId* old_address,
  xct::WriteXctAccess* write_set) {
  return MasstreeStoragePimpl(this).track_moved_record(old_address, write_set);
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
  (thread::Thread* context, const RecordLocation& location, \
  const void* be_key, KeyLength key_length, x* value, PayloadLength payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);
// @endcond

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
