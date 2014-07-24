/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_page_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>

#include "foedus/engine.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/rdtsc_watch.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {

void MasstreePage::initialize_volatile_common(
  StorageId           storage_id,
  VolatilePagePointer page_id,
  PageType            page_type,
  uint8_t             layer,
  bool                root_in_layer,
  KeySlice            low_fence,
  KeySlice            high_fence,
  bool                is_high_fence_supremum,
  KeySlice            foster_fence,
  MasstreePage*       foster_child,
  bool                initially_locked) {
  header_.init_volatile(
    page_id,
    storage_id,
    page_type,
    root_in_layer && layer == 0);  // the true root is only the one in layer-0
  uint64_t ver = (layer << kPageVersionLayerShifts);
  if (initially_locked) {
    ver |= kPageVersionLockedBit;
  }
  if (foster_child) {
    ver |= kPageVersionHasFosterChildBit;
  }
  if (page_type == kMasstreeBorderPageType) {
    ver |= kPageVersionIsBorderBit;
  }
  if (is_high_fence_supremum) {
    ver |= kPageVersionIsSupremumBit;
  }
  header_.page_version_.set_data(ver);
  high_fence_ = high_fence;
  low_fence_ = low_fence;
  foster_fence_ = foster_fence;
  foster_child_ = foster_child;
}

void MasstreeBorderPage::initialize_volatile_page(
  StorageId           storage_id,
  VolatilePagePointer page_id,
  uint8_t             layer,
  bool                root_in_layer,
  KeySlice            low_fence,
  KeySlice            high_fence,
  bool                is_high_fence_supremum,
  KeySlice            foster_fence,
  MasstreePage*       foster_child,
  bool                initially_locked) {
  initialize_volatile_common(
    storage_id,
    page_id,
    kMasstreeBorderPageType,
    layer,
    root_in_layer,
    low_fence,
    high_fence,
    is_high_fence_supremum,
    foster_fence,
    foster_child,
    initially_locked);
}

void MasstreePage::release_pages_recursive_common(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  if (header_.get_page_type() == kMasstreeBorderPageType) {
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(this);
    casted->release_pages_recursive(page_resolver, batch);
  } else {
    ASSERT_ND(header_.get_page_type() == kMasstreeIntermediatePageType);
    MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(this);
    casted->release_pages_recursive(page_resolver, batch);
  }
}

void MasstreeIntermediatePage::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  if (foster_child_) {
    reinterpret_cast<MasstreeIntermediatePage*>(foster_child_)->release_pages_recursive(
      page_resolver,
      batch);
    foster_child_ = nullptr;
  }
  uint16_t key_count = get_version().get_key_count();
  ASSERT_ND(key_count <= kMaxIntermediateSeparators);
  for (uint8_t i = 0; i < key_count + 1; ++i) {
    MiniPage& minipage = get_minipage(i);
    uint16_t mini_count = minipage.mini_version_.get_key_count();
    ASSERT_ND(mini_count <= kMaxIntermediateMiniSeparators);
    for (uint8_t j = 0; j < mini_count + 1; ++j) {
      VolatilePagePointer& pointer = minipage.pointers_[j].volatile_pointer_;
      if (pointer.components.offset != 0) {
        MasstreePage* child = reinterpret_cast<MasstreePage*>(
          page_resolver.resolve_offset(pointer));
        child->release_pages_recursive_common(page_resolver, batch);
        pointer.components.offset = 0;
      }
    }
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  batch->release(volatile_id);
}

void MasstreeBorderPage::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  if (foster_child_) {
    reinterpret_cast<MasstreeBorderPage*>(foster_child_)->release_pages_recursive(
      page_resolver,
      batch);
    foster_child_ = nullptr;
  }
  uint16_t key_count = get_version().get_key_count();
  ASSERT_ND(key_count <= kMaxKeys);
  for (uint8_t i = 0; i < key_count; ++i) {
    if (does_point_to_layer(i)) {
      DualPagePointer& pointer = *get_next_layer(i);
      if (pointer.volatile_pointer_.components.offset != 0) {
        MasstreePage* child = reinterpret_cast<MasstreePage*>(
          page_resolver.resolve_offset(pointer.volatile_pointer_));
        child->release_pages_recursive_common(page_resolver, batch);
        pointer.volatile_pointer_.components.offset = 0;
      }
    }
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  batch->release(volatile_id);
}


void MasstreeBorderPage::copy_initial_record(
  const MasstreeBorderPage* copy_from,
  uint8_t copy_index) {
  ASSERT_ND(header_.page_version_.get_key_count() == 0);
  ASSERT_ND(is_locked());
  ASSERT_ND(copy_from->is_locked());
  uint8_t parent_key_length = copy_from->remaining_key_length_[copy_index];
  ASSERT_ND(parent_key_length != kKeyLengthNextLayer);
  ASSERT_ND(parent_key_length > sizeof(KeySlice));
  uint8_t remaining = parent_key_length - sizeof(KeySlice);

  // retrieve the first 8 byte (or less) as the new slice.
  const char* parent_record = copy_from->get_record(copy_index);
  KeySlice new_slice = slice_key(parent_record, remaining);
  uint16_t payload_length = copy_from->payload_length_[copy_index];
  uint8_t suffix_length = calculate_suffix_length(remaining);

  slices_[0] = new_slice;
  remaining_key_length_[0] = remaining;
  payload_length_[0] = payload_length;
  offsets_[0] = (kDataSize - calculate_record_size(remaining, payload_length)) >> 4;

  // use the same xct ID. This means we also inherit deleted flag.
  owner_ids_[0] = copy_from->owner_ids_[copy_index];
  // but we don't want to inherit locks
  if (owner_ids_[0].is_keylocked()) {
    owner_ids_[0].release_keylock();
  }
  if (owner_ids_[0].is_rangelocked()) {
    owner_ids_[0].release_rangelock();
  }
  if (suffix_length > 0) {
    std::memcpy(get_record(0), parent_record + sizeof(KeySlice), suffix_length);
  }
  std::memcpy(
    get_record(0) + suffix_length,
    parent_record + remaining,
    payload_length);

  header_.page_version_.increment_key_count();
}

ErrorCode MasstreeBorderPage::split_foster(thread::Thread* context, KeySlice trigger) {
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(is_locked());
  debugging::RdtscWatch watch;

  uint8_t key_count = header_.page_version_.get_key_count();
  header_.page_version_.set_splitting();

  DVLOG(1) << "Splitting a page... ";

  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  SplitStrategy strategy = split_foster_decide_strategy(key_count, trigger);

  // from now on no failure possible.
  MasstreeBorderPage* new_page = reinterpret_cast<MasstreeBorderPage*>(
    context->get_local_volatile_page_resolver().resolve_offset(offset));
  VolatilePagePointer new_pointer;
  new_pointer = combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offset);
  new_page->initialize_volatile_page(
    header_.storage_id_,
    new_pointer,
    get_layer(),
    false,
    strategy.mid_slice_,  // the new separator is the low fence of new page
    high_fence_,          // high fence is same as this page
    is_high_fence_supremum(),
    foster_fence_,  // inherit foster key and child
    foster_child_,
    true);  // yes, lock it
  ASSERT_ND(new_page->is_locked());
  if (strategy.no_record_split_) {
    // this one is easy because we don't have to move anything. even no lock.
    // actually, nothing to do here!
  } else {
    // otherwise, we have to lock all records.
    // we can instead lock only moving records, but in reality we are moving almost all records
    // because of compaction. so just do it all.
    split_foster_lock_existing_records(key_count);
    // commit the system transaction to get xct_id
    xct::XctId new_id = split_foster_commit_system_xct(context, strategy);

    // from now on, it's kind of "apply" phase
    new_page->split_foster_migrate_records(new_id, strategy, this);

    // release all record locks.
    assorted::memory_fence_release();
    for (uint8_t i = 0; i < key_count; ++i) {
      owner_ids_[i] = new_id;  // unlock
    }
    assorted::memory_fence_release();
  }

  foster_child_ = new_page;
  foster_fence_ = strategy.mid_slice_;
  if (!header_.page_version_.has_foster_child()) {
    header_.page_version_.set_has_foster_child(true);
  }

  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to split a page. original page physical"
    << " record count: " << key_count << "->" << header_.page_version_.get_key_count();
  return kErrorCodeOk;
}

SplitStrategy MasstreeBorderPage::split_foster_decide_strategy(
  uint8_t key_count,
  KeySlice trigger) const {
  SplitStrategy ret;
  ret.original_key_count_ = key_count;
  ret.no_record_split_ = false;
  ret.smallest_slice_ = slices_[0];
  ret.largest_slice_ = slices_[0];
  uint8_t inorder_count = 0;
  for (uint8_t i = 1; i < key_count; ++i) {
    if (slices_[i] <= ret.smallest_slice_) {
      ret.smallest_slice_ = slices_[i];
    } else if (slices_[i] >= ret.largest_slice_) {
      ret.largest_slice_ = slices_[i];
      ++inorder_count;
    }
  }

  if (trigger > ret.largest_slice_ &&
      inorder_count >= static_cast<uint32_t>(key_count) * 120U / 128U) {
    // let's do no-record split
    ret.no_record_split_ = true;
    DVLOG(1) << "Yay, no record split. inorder_count=" << inorder_count
      << " key_count=" << key_count;
    ret.mid_slice_ = ret.largest_slice_ + 1;
    return ret;
  } else if (trigger == ret.largest_slice_) {
    DVLOG(1) << "Not a no record split, but still quite skewed. inorder_count=" << inorder_count
      << " key_count=" << key_count;
    ret.mid_slice_ = ret.largest_slice_;
    return ret;
  }

  // now we have to pick separator. as we don't sort in-page, this is approximate median selection.
  // there are a few smart algorithm out there, but we don't need that much accuracy.
  // just randomly pick a few. good enough.
  assorted::UniformRandom uniform_random(12345);
  const uint8_t kSamples = 7;
  KeySlice choices[kSamples];
  for (uint8_t i = 0; i < kSamples; ++i) {
    choices[i] = slices_[uniform_random.uniform_within(0, key_count - 1)];
  }
  std::sort(choices, choices + kSamples);
  ret.mid_slice_ = choices[kSamples / 2];
  return ret;
}

void MasstreeBorderPage::split_foster_lock_existing_records(uint8_t key_count) {
  debugging::RdtscWatch watch;  // check how expensive this is
  for (uint8_t i = 0; i < key_count; ++i) {
    // lock in address order. so, no deadlock possible
    owner_ids_[i].keylock_unconditional();
    // we have to lock them whether the record is deleted or not. all physical records.
  }
  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to lock all of " << key_count
    << " records while splitting";
  if (watch.elapsed() > (1ULL << 26)) {
    // if we see this often, we have to optimize this somehow.
    LOG(WARNING) << "wait, wait, it costed " << watch.elapsed() << " cycles to lock all of "
      << key_count << " records while splitting!! that's a lot!";
  }
}

xct::XctId MasstreeBorderPage::split_foster_commit_system_xct(
  thread::Thread* context,
  const SplitStrategy &strategy) const {
  // although this is a no-log system transaction, the protocol to determine xct_id is same.
  xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
  assorted::memory_fence_acq_rel();
  // as a system transaction, this is the serialization point
  Epoch current_epoch = xct_manager.get_current_global_epoch();
  assorted::memory_fence_acq_rel();

  xct::XctId new_id = context->get_current_xct().get_id();
  if (new_id.get_epoch().before(current_epoch)) {
    new_id.set_epoch(current_epoch);
    new_id.set_ordinal(0);
  }
  for (uint8_t i = 0; i < strategy.original_key_count_; ++i) {
    new_id.store_max(owner_ids_[i]);
  }
  if (UNLIKELY(new_id.get_ordinal() == 0xFFFFU)) {
    xct_manager.advance_current_global_epoch();
    ASSERT_ND(current_epoch.before(xct_manager.get_current_global_epoch()));
    current_epoch = xct_manager.get_current_global_epoch();
    new_id.set_epoch(current_epoch);
    new_id.set_ordinal(0);
  }
  ASSERT_ND(new_id.get_ordinal() < 0xFFFFU);
  new_id.clear_status_bits();
  new_id.set_ordinal(new_id.get_ordinal() + 1);
  new_id.set_thread_id(context->get_thread_id());
  context->get_current_xct().remember_previous_xct_id(new_id);
  return new_id;
}

void MasstreeBorderPage::split_foster_migrate_records(
  xct::XctId xct_id,
  const SplitStrategy &strategy,
  MasstreeBorderPage* parent) {
  ASSERT_ND(header_.page_version_.get_key_count() == 0);
  uint8_t migrated_count = 0;
  uint16_t unused_space = kDataSize;
  for (uint8_t i = 0; i < strategy.original_key_count_; ++i) {
    if (parent->slices_[i] >= strategy.mid_slice_) {
      // move this record.
      slices_[migrated_count] = parent->slices_[i];
      remaining_key_length_[migrated_count] = parent->remaining_key_length_[i];
      payload_length_[migrated_count] = parent->payload_length_[i];
      owner_ids_[migrated_count] = xct_id;

      uint16_t record_length = sizeof(DualPagePointer);
      if (remaining_key_length_[migrated_count] != kKeyLengthNextLayer) {
        record_length = calculate_record_size(
          remaining_key_length_[migrated_count],
          payload_length_[migrated_count]);
      }
      ASSERT_ND(unused_space >= record_length);
      unused_space -= record_length;
      offsets_[migrated_count] = unused_space >> 4;
      std::memcpy(get_record(migrated_count), parent->get_record(i), record_length);
      ++migrated_count;
    }
  }
  header_.page_version_.set_key_count(migrated_count);

  // compact the parent page, too
  uint8_t deleted_count = 0;
  uint8_t compacted_count = 0;
  uint16_t compacted_space = kDataSize;
  // here, we utilize the fact that offset_[i] > offset_[j] iff i < j.
  // in other words, later records never overwrite previously compacted records.
  for (uint8_t i = 0; i < strategy.original_key_count_; ++i) {
    if (parent->owner_ids_[i].is_deleted()) {
      ++deleted_count;  // deleted records are compacted, in other words physically deleted.
    } else if (parent->slices_[i] < strategy.mid_slice_) {
      parent->slices_[compacted_count] = parent->slices_[i];
      parent->remaining_key_length_[compacted_count] = parent->remaining_key_length_[i];
      parent->payload_length_[compacted_count] = parent->payload_length_[i];

      uint16_t record_length = sizeof(DualPagePointer);
      if (parent->remaining_key_length_[migrated_count] != kKeyLengthNextLayer) {
        record_length = calculate_record_size(
          parent->remaining_key_length_[migrated_count],
          parent->payload_length_[migrated_count]);
      }
      ASSERT_ND(compacted_space >= record_length);
      compacted_space -= record_length;
      parent->offsets_[compacted_count] = compacted_space >> 4;
      std::memmove(parent->get_record(compacted_count), parent->get_record(i), record_length);
      ++compacted_count;
    }
  }
  parent->header_.page_version_.set_key_count(compacted_count);
  ASSERT_ND(deleted_count + migrated_count + compacted_count == strategy.original_key_count_);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
