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
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
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
  bool                initially_locked) {
  // std::memset(this, 0, kPageSize);  // expensive
  header_.init_volatile(page_id, storage_id, page_type, root_in_layer);
  uint64_t ver = (layer << kPageVersionLayerShifts);
  if (initially_locked) {
    ver |= kPageVersionLockedBit;
  }
  if (is_high_fence_supremum) {
    ver |= kPageVersionIsSupremumBit;
  }
  if (root_in_layer) {
    ver |= kPageVersionIsRootBit;
  }
  header_.page_version_.set_data(ver);
  high_fence_ = high_fence;
  low_fence_ = low_fence;
  foster_fence_ = low_fence;
  foster_twin_[0] = nullptr;
  foster_twin_[1] = nullptr;
  ASSERT_ND(header_.page_version_.get_key_count() == 0);
}

void MasstreeIntermediatePage::initialize_volatile_page(
  StorageId           storage_id,
  VolatilePagePointer page_id,
  uint8_t             layer,
  bool                root_in_layer,
  KeySlice            low_fence,
  KeySlice            high_fence,
  bool                is_high_fence_supremum,
  bool                initially_locked) {
  initialize_volatile_common(
    storage_id,
    page_id,
    kMasstreeIntermediatePageType,
    layer,
    root_in_layer,
    low_fence,
    high_fence,
    is_high_fence_supremum,
    initially_locked);
  for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    get_minipage(i).key_count_ = 0;
  }
}

void MasstreeBorderPage::initialize_volatile_page(
  StorageId           storage_id,
  VolatilePagePointer page_id,
  uint8_t             layer,
  bool                root_in_layer,
  KeySlice            low_fence,
  KeySlice            high_fence,
  bool                is_high_fence_supremum,
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
  for (int i = 0; i < 2; ++i) {
    if (foster_twin_[i]) {
      reinterpret_cast<MasstreeIntermediatePage*>(foster_twin_[i])->release_pages_recursive(
        page_resolver,
        batch);
      foster_twin_[i] = nullptr;
    }
  }
  uint16_t key_count = get_version().get_key_count();
  ASSERT_ND(key_count <= kMaxIntermediateSeparators);
  for (uint8_t i = 0; i < key_count + 1; ++i) {
    MiniPage& minipage = get_minipage(i);
    uint16_t mini_count = minipage.key_count_;
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
  for (int i = 0; i < 2; ++i) {
    if (foster_twin_[i]) {
      reinterpret_cast<MasstreeBorderPage*>(foster_twin_[i])->release_pages_recursive(
        page_resolver,
        batch);
      foster_twin_[i] = nullptr;
    }
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


void MasstreeBorderPage::initialize_layer_root(
  const MasstreeBorderPage* copy_from,
  uint8_t copy_index) {
  ASSERT_ND(header_.page_version_.get_key_count() == 0);
  ASSERT_ND(is_locked());
  ASSERT_ND(copy_from->get_owner_id(copy_index)->is_keylocked());
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

inline ErrorCode grab_two_free_pages(thread::Thread* context, memory::PagePoolOffset* offsets) {
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  offsets[0] = memory->grab_free_volatile_page();
  if (offsets[0] == 0) {
    return kErrorCodeMemoryNoFreePages;
  }
  offsets[1] = memory->grab_free_volatile_page();
  if (offsets[1] == 0) {
    memory->release_free_volatile_page(offsets[0]);
    return kErrorCodeMemoryNoFreePages;
  }
  return kErrorCodeOk;
}

/////////////////////////////////////////////////////////////////////////////////////
///
///                      Border node's Split
///
/////////////////////////////////////////////////////////////////////////////////////

ErrorCode MasstreeBorderPage::split_foster(
  thread::Thread* context,
  KeySlice trigger,
  MasstreeBorderPage** target) {
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(is_locked());
  ASSERT_ND(!is_moved());
  ASSERT_ND(foster_twin_[0] == nullptr && foster_twin_[1] == nullptr);  // same as !is_moved()
  debugging::RdtscWatch watch;

  uint8_t key_count = header_.page_version_.get_key_count();
  DVLOG(1) << "Splitting a page... ";

  memory::PagePoolOffset offsets[2];
  CHECK_ERROR_CODE(grab_two_free_pages(context, offsets));

  // from now on no failure possible.
  BorderSplitStrategy strategy = split_foster_decide_strategy(key_count, trigger);
  MasstreeBorderPage* twin[2];
  for (int i = 0; i < 2; ++i) {
    twin[i] = reinterpret_cast<MasstreeBorderPage*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(offsets[i]));
    foster_twin_[i] = twin[i];
    twin[i]->initialize_volatile_page(
      header_.storage_id_,
      combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offsets[i]),
      get_layer(),
      false,
      i == 0 ? low_fence_ : strategy.mid_slice_,  // low-fence
      i == 0 ? strategy.mid_slice_ : high_fence_,  // high-fence
      i == 0 ? false : is_high_fence_supremum(),  // high-fence supremum
      true);  // yes, lock it
    ASSERT_ND(twin[i]->is_locked());
  }
  split_foster_lock_existing_records(context, key_count);
  // commit the system transaction to get xct_id
  xct::XctId new_id = split_foster_commit_system_xct(context, strategy);
  if (strategy.no_record_split_) {
    // in this case, we can move all records in one memcpy.
    // copy everything from the end of header to the end of page
    std::memcpy(
      &(twin[0]->remaining_key_length_),
      &(remaining_key_length_),
      kPageSize - sizeof(MasstreePage));
    for (uint8_t i = 0; i < key_count; ++i) {
      ASSERT_ND(twin[0]->owner_ids_[i].is_keylocked());
      twin[0]->owner_ids_[i].release_keylock();
    }
    twin[0]->get_version().set_key_count(key_count);
    twin[1]->get_version().set_key_count(0);
  } else {
    twin[0]->split_foster_migrate_records(
      *this,
      key_count,
      strategy.smallest_slice_,
      strategy.mid_slice_ - 1);  // to make it inclusive
    twin[1]->split_foster_migrate_records(
      *this,
      key_count,
      strategy.mid_slice_,
      strategy.largest_slice_);  // this is inclusive (to avoid supremum hassles)
  }

  // release all record locks, but set the "moved" bit so that concurrent transactions
  // check foster-twin for read-set/write-set checks.
  new_id.set_moved();
  for (uint8_t i = 0; i < key_count; ++i) {
    owner_ids_[i] = new_id;  // unlock and also notify that the record has been moved
  }

  // this page is now "moved".
  // which will be the target page?
  get_version().set_moved();
  get_version().set_has_foster_child(true);
  foster_fence_ = strategy.mid_slice_;
  if (within_foster_minor(trigger)) {
    *target = twin[0];
    twin[1]->unlock();
  } else {
    ASSERT_ND(within_foster_major(trigger));
    *target = twin[1];
    twin[0]->unlock();
  }

  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to split a page. original page physical"
    << " record count: " << static_cast<int>(key_count)
    << "->" << header_.page_version_.get_key_count();
  return kErrorCodeOk;
}

BorderSplitStrategy MasstreeBorderPage::split_foster_decide_strategy(
  uint8_t key_count,
  KeySlice trigger) const {
  BorderSplitStrategy ret;
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
    DVLOG(1) << "Yay, no record split. inorder_count=" << static_cast<int>(inorder_count)
      << " key_count=" << static_cast<int>(key_count);
    ret.mid_slice_ = ret.largest_slice_ + 1;
    return ret;
  } else if (trigger == ret.largest_slice_) {
    DVLOG(1) << "Not a no record split, but still quite skewed. inorder_count="
      << static_cast<int>(inorder_count) << " key_count=" << static_cast<int>(key_count);
    ret.mid_slice_ = ret.largest_slice_ + 1;
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

  // scan through again to make sure the new separator is not used multiple times as key.
  // this is required for the invariant "same slices must be in same page"
  while (true) {
    bool observed = false;
    bool retry = false;
    for (uint8_t i = 0; i < key_count; ++i) {
      if (slices_[i] == ret.mid_slice_) {
        if (observed) {
          // the key appeared twice! let's try another slice.
          ++ret.mid_slice_;
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
  return ret;
}

void MasstreeBorderPage::split_foster_lock_existing_records(
  thread::Thread* context,
  uint8_t key_count) {
  debugging::RdtscWatch watch;  // check how expensive this is
  // lock in address order. so, no deadlock possible
  // we have to lock them whether the record is deleted or not. all physical records.
  xct::XctId::keylock_unconditional_batch(owner_ids_, key_count);  // lock using batches
  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to lock all of "
    << static_cast<int>(key_count) << " records while splitting";
  if (watch.elapsed() > (1ULL << 26)) {
    // if we see this often, we have to optimize this somehow.
    LOG(WARNING) << "wait, wait, it costed " << watch.elapsed() << " cycles to lock all of "
      << static_cast<int>(key_count) << " records while splitting!! that's a lot! storage="
      << context->get_engine()->get_storage_manager().get_storage(header_.storage_id_)->get_name()
      << ", thread ID=" << context->get_thread_id();
  }
}

xct::XctId MasstreeBorderPage::split_foster_commit_system_xct(
  thread::Thread* context,
  const BorderSplitStrategy &strategy) const {
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
  const MasstreeBorderPage& copy_from,
  uint8_t key_count,
  KeySlice inclusive_from,
  KeySlice inclusive_to) {
  ASSERT_ND(header_.page_version_.get_key_count() == 0);
  uint8_t migrated_count = 0;
  uint16_t unused_space = kDataSize;
  // utilize the fact that records grow backwards.
  // memcpy contiguous records as much as possible
  uint16_t contiguous_copy_size = 0;
  uint16_t contiguous_copy_to_begin = 0;
  uint16_t contiguous_copy_from_begin = 0;
  for (uint8_t i = 0; i < key_count; ++i) {
    if (copy_from.slices_[i] >= inclusive_from && copy_from.slices_[i] <= inclusive_to) {
      // move this record.
      slices_[migrated_count] = copy_from.slices_[i];
      remaining_key_length_[migrated_count] = copy_from.remaining_key_length_[i];
      payload_length_[migrated_count] = copy_from.payload_length_[i];
      owner_ids_[migrated_count] = copy_from.owner_ids_[i];
      ASSERT_ND(owner_ids_[migrated_count].is_keylocked());
      owner_ids_[migrated_count].release_keylock();

      uint16_t record_length = sizeof(DualPagePointer);
      if (remaining_key_length_[migrated_count] != kKeyLengthNextLayer) {
        record_length = calculate_record_size(
          remaining_key_length_[migrated_count],
          payload_length_[migrated_count]);
      }
      ASSERT_ND(unused_space >= record_length);
      unused_space -= record_length;
      offsets_[migrated_count] = unused_space >> 4;

      uint16_t copy_from_begin = static_cast<uint16_t>(copy_from.offsets_[i]) << 4;
      if (contiguous_copy_size == 0) {
        contiguous_copy_size = record_length;
        contiguous_copy_from_begin = copy_from_begin;
        contiguous_copy_to_begin = unused_space;
      } else if (contiguous_copy_from_begin - record_length != copy_from_begin) {
        // this happens when the record has shrunk (eg now points to next layer).
        // flush contiguous data.
        ASSERT_ND(contiguous_copy_from_begin - record_length > copy_from_begin);
        std::memcpy(
          data_ + contiguous_copy_to_begin,
          copy_from.data_ + contiguous_copy_from_begin,
          contiguous_copy_size);
        contiguous_copy_size = record_length;
        contiguous_copy_from_begin = copy_from_begin;
        contiguous_copy_to_begin = unused_space;
      } else {
        ASSERT_ND(contiguous_copy_from_begin - record_length == copy_from_begin);
        ASSERT_ND(contiguous_copy_to_begin >= record_length);
        contiguous_copy_size += record_length;
        contiguous_copy_from_begin = copy_from_begin;
        contiguous_copy_to_begin -= record_length;
      }
      ++migrated_count;
    } else {
      // oh, we didn't copy this record, so the contiguity is broken. do the copy now.
      if (contiguous_copy_size > 0U) {
        std::memcpy(
          data_ + contiguous_copy_to_begin,
          copy_from.data_ + contiguous_copy_from_begin,
          contiguous_copy_size);
        contiguous_copy_size = 0;
      }
    }
  }
  // after all, do the copy now.
  if (contiguous_copy_size > 0U) {
    std::memcpy(
      data_ + contiguous_copy_to_begin,
      copy_from.data_ + contiguous_copy_from_begin,
      contiguous_copy_size);
  }

  header_.page_version_.set_key_count(migrated_count);
}

/////////////////////////////////////////////////////////////////////////////////////
///
///                      Interior node's Split
///
/////////////////////////////////////////////////////////////////////////////////////
ErrorCode MasstreeIntermediatePage::split_foster_and_adopt(
  thread::Thread* context,
  MasstreePage* trigger_child) {
  // similar to border page's split, but simpler in a few places because
  // 1) intermediate page doesn't have owner_id for each pointer (no lock concerns).
  // 2) intermediate page is already completely sorted.
  // thus, this is just a physical operation without any transactional behavior.
  // even not a system transaction
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(is_locked());
  ASSERT_ND(!is_moved());
  ASSERT_ND(foster_twin_[0] == nullptr && foster_twin_[1] == nullptr);  // same as !is_moved()
  debugging::RdtscWatch watch;

  trigger_child->lock();
  UnlockScope trigger_scope(trigger_child);
  if (trigger_child->is_retired()) {
    VLOG(0) << "Interesting. this child is now retired, so someone else has already adopted.";
    return kErrorCodeOk;  // fine. the goal is already achieved
  }

  uint8_t key_count = header_.page_version_.get_key_count();
  ASSERT_ND(key_count == kMaxIntermediateSeparators);
  DVLOG(1) << "Splitting an intermediate page... ";
  verify_separators();

  memory::PagePoolOffset offsets[2];
  CHECK_ERROR_CODE(grab_two_free_pages(context, offsets));
  memory::NumaCoreMemory* memory = context->get_thread_memory();

  memory::PagePoolOffset work_offset = memory->grab_free_volatile_page();
  if (work_offset == 0) {
    memory->release_free_volatile_page(offsets[0]);
    memory->release_free_volatile_page(offsets[1]);
    return kErrorCodeMemoryNoFreePages;
  }

  // from now on no failure possible.
  // it might be a sorted insert.
  KeySlice new_foster_fence;
  bool no_record_split = false;
  const MiniPage& last_minipage = get_minipage(key_count);
  IntermediateSplitStrategy* strategy = nullptr;
  if (last_minipage.key_count_ > 0 &&
    trigger_child->get_foster_fence() > last_minipage.separators_[last_minipage.key_count_ - 1]) {
    DVLOG(0) << "Seems like a sequential insert. let's do no-record split";
    no_record_split = true;
    // triggering key as new separator (remember, low-fence is inclusive)
    new_foster_fence = trigger_child->get_foster_fence();
  } else {
    strategy = reinterpret_cast<IntermediateSplitStrategy*>(
        context->get_local_volatile_page_resolver().resolve_offset_newpage(work_offset));
    ASSERT_ND(sizeof(IntermediateSplitStrategy) <= kPageSize);
    split_foster_decide_strategy(strategy);
    new_foster_fence = strategy->mid_separator_;  // the new separator is the low fence of new page
  }

  MasstreeIntermediatePage* twin[2];
  for (int i = 0; i < 2; ++i) {
    twin[i] = reinterpret_cast<MasstreeIntermediatePage*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(offsets[i]));
    foster_twin_[i] = twin[i];
    VolatilePagePointer new_pointer = combine_volatile_page_pointer(
      context->get_numa_node(), 0, 0, offsets[i]);

    twin[i]->initialize_volatile_page(
      header_.storage_id_,
      new_pointer,
      get_layer(),
      false,
      i == 0 ? low_fence_ : new_foster_fence,
      i == 0 ? new_foster_fence : high_fence_,
      i == 0 ? false : is_high_fence_supremum(),
      true);  // yes, lock it
    ASSERT_ND(twin[i]->is_locked());
  }


  if (!no_record_split) {
    // reconstruct both old page and new page.
    // we are copying contents from the strategy object, so no worry on overwritten source.
    twin[0]->split_foster_migrate_records(*strategy, 0, strategy->mid_index_ + 1, new_foster_fence);
    twin[1]->split_foster_migrate_records(
      *strategy,
      strategy->mid_index_ + 1,
      strategy->total_separator_count_,
      high_fence_);
    // in this case, we don't bother adopting foster twin of trigger_child.
    // the next traversal would do it.
  } else {
    // in this case, we can move all data in one memcpy.
    // copy everything from the end of header to the end of page
    std::memcpy(&(twin[0]->separators_), &(separators_), kPageSize - sizeof(MasstreePage));
    twin[0]->get_version().set_key_count(key_count);
    twin[1]->get_version().set_key_count(0);
    ASSERT_ND(new_foster_fence == trigger_child->get_foster_fence());

    // also adopt foster twin of trigger_child
    DualPagePointer& major_pointer = twin[1]->get_minipage(0).pointers_[0];
    major_pointer.snapshot_pointer_ = 0;
    major_pointer.volatile_pointer_.word = trigger_child->get_foster_major()->header().page_id_;
    MiniPage& new_minipage = twin[0]->get_minipage(key_count);
    DualPagePointer& old_pointer = new_minipage.pointers_[new_minipage.key_count_];
    ASSERT_ND(context->get_global_volatile_page_resolver().resolve_offset(
      old_pointer.volatile_pointer_) == reinterpret_cast<Page*>(trigger_child));
    old_pointer.snapshot_pointer_ = 0;
    old_pointer.volatile_pointer_.word = trigger_child->get_foster_minor()->header().page_id_;

    ASSERT_ND(reinterpret_cast<MasstreePage*>(
      context->get_global_volatile_page_resolver().resolve_offset(major_pointer.volatile_pointer_))
      == trigger_child->get_foster_major());
    ASSERT_ND(reinterpret_cast<MasstreePage*>(
      context->get_global_volatile_page_resolver().resolve_offset(old_pointer.volatile_pointer_))
      == trigger_child->get_foster_minor());
  }

  for (int i = 0; i < 2; ++i) {
    twin[i]->unlock();
  }

  if (no_record_split) {
    // trigger_child is retired.  TODO(Hideaki) GC
    trigger_child->get_version().set_retired();
  }

  get_version().set_moved();
  get_version().set_has_foster_child(true);
  foster_fence_ = new_foster_fence;

  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to split a node. original node"
    << " key count: " << static_cast<int>(key_count)
    << "->" << header_.page_version_.get_key_count()
    << (no_record_split ? " no record split" : " usual split");
  memory->release_free_volatile_page(work_offset);

  verify_separators();
  return kErrorCodeOk;
}

void MasstreeIntermediatePage::split_foster_decide_strategy(IntermediateSplitStrategy* out) const {
  ASSERT_ND(is_locked());
  out->total_separator_count_ = 0;
  uint8_t key_count = get_version().get_key_count();
  for (uint8_t i = 0; i <= key_count; ++i) {
    const MiniPage& mini_page = get_minipage(i);
    uint8_t separator_count = mini_page.key_count_;
    for (uint8_t j = 0; j < separator_count; ++j) {
      ASSERT_ND(out->total_separator_count_ == 0 ||
        out->separators_[out->total_separator_count_ - 1] < mini_page.separators_[j]);
      out->separators_[out->total_separator_count_] = mini_page.separators_[j];
      out->pointers_[out->total_separator_count_] = mini_page.pointers_[j];
      ++(out->total_separator_count_);
      ASSERT_ND(out->total_separator_count_ < IntermediateSplitStrategy::kMaxSeparators);
    }
    if (i == key_count) {
      ASSERT_ND(out->total_separator_count_ == 0 ||
        out->separators_[out->total_separator_count_ - 1] < high_fence_);
      out->separators_[out->total_separator_count_] = high_fence_;
    } else {
      ASSERT_ND(out->total_separator_count_ == 0 ||
        out->separators_[out->total_separator_count_ - 1] < separators_[i]);
      out->separators_[out->total_separator_count_] = separators_[i];
    }
    out->pointers_[out->total_separator_count_] = mini_page.pointers_[separator_count];
    ++(out->total_separator_count_);
    ASSERT_ND(out->total_separator_count_ < IntermediateSplitStrategy::kMaxSeparators);
  }
  out->mid_index_ = out->total_separator_count_ / 2;
  out->mid_separator_ = out->separators_[out->mid_index_];
}

void MasstreeIntermediatePage::split_foster_migrate_records(
  const IntermediateSplitStrategy &strategy,
  uint16_t from,
  uint16_t to,
  KeySlice expected_last_separator) {
  ASSERT_ND(is_locked());

  // construct this page. copy the separators and pointers.
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
  MiniPage* cur_mini_page = &get_minipage(0);
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

      separators_[cur_mini] = next_separator;

      next_mini_threshold += entries_per_mini;
      cur_mini_separators = 0;
      ++cur_mini;
      cur_mini_page = &get_minipage(cur_mini);
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
  get_version().set_key_count(cur_mini);  // set key count after all
  ASSERT_ND(get_version().get_key_count() <= kMaxIntermediateSeparators);

  // the last separator is ignored because it's foster-fence/high-fence.
  ASSERT_ND(next_separator == expected_last_separator);

  verify_separators();
}

void MasstreeIntermediatePage::verify_separators() const {
#ifndef NDEBUG
  for (uint8_t i = 0; i <= get_version().get_key_count(); ++i) {
    KeySlice low, high;
    if (i < get_version().get_key_count()) {
      if (i > 0) {
        low = separators_[i - 1];
      } else {
        low = low_fence_;
      }
      high = separators_[i];
      ASSERT_ND(separators_[i] > low);
    } else {
      low = separators_[get_version().get_key_count() - 1];
      high = high_fence_;
    }
    const MiniPage& minipage = get_minipage(i);
    for (uint8_t j = 0; j <= minipage.key_count_; ++j) {
      ASSERT_ND(!minipage.pointers_[j].is_both_null());
      if (j < minipage.key_count_) {
        ASSERT_ND(minipage.separators_[j] > low);
        ASSERT_ND(minipage.separators_[j] < high);
      }
    }
  }
#endif  // NDEBUG
}

/////////////////////////////////////////////////////////////////////////////////////
///
///                      Interior node's Local Rebalance
///
/////////////////////////////////////////////////////////////////////////////////////

ErrorCode MasstreeIntermediatePage::local_rebalance(thread::Thread* context) {
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(!is_moved());
  ASSERT_ND(!is_retired());
  ASSERT_ND(is_locked());
  debugging::RdtscWatch watch;

  uint8_t key_count = header_.page_version_.get_key_count();
  DVLOG(1) << "Rebalancing an intermediate page... ";

  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset work_offset = memory->grab_free_volatile_page();
  if (work_offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  // from now on no failure possible.
  // reuse the code of split.
  IntermediateSplitStrategy* strategy =
    reinterpret_cast<IntermediateSplitStrategy*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(work_offset));
  split_foster_decide_strategy(strategy);

  // reconstruct this page.
  uint16_t count = strategy->total_separator_count_;
  split_foster_migrate_records(*strategy, 0, count, high_fence_);

  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to rebalance a node. original"
    << " key count: " << static_cast<int>(key_count)
    << "->" << header_.page_version_.get_key_count()
    << ", total separator count=" << count;
  memory->release_free_volatile_page(work_offset);
  verify_separators();
  return kErrorCodeOk;
}

/////////////////////////////////////////////////////////////////////////////////////
///
///                      Interior node's Adopt
///
/////////////////////////////////////////////////////////////////////////////////////

ErrorCode MasstreeIntermediatePage::adopt_from_child(
  thread::Thread* context,
  KeySlice searching_slice,
  uint8_t minipage_index,
  uint8_t pointer_index,
  MasstreePage* child) {
  ASSERT_ND(!is_retired());
  lock();
  UnlockScope scope(this);
  if (is_moved()) {
    VLOG(0) << "Interesting. concurrent thread has already split this node? retry";
    return kErrorCodeOk;
  }

  uint8_t key_count = get_version().get_key_count();
  MiniPage& minipage = get_minipage(minipage_index);
  ASSERT_ND(minipage.key_count_ <= kMaxIntermediateMiniSeparators);
  {
    if (minipage_index > key_count || pointer_index > minipage.key_count_) {
      VLOG(0) << "Interesting. there seems some change in this interior page. retry adoption";
      return kErrorCodeOk;
    }

    // TODO(Hideaki) let's make this a function.
    KeySlice separator_low;
    KeySlice separator_high;
    if (pointer_index == 0) {
      if (minipage_index == 0) {
        separator_low = low_fence_;
      } else {
        separator_low = separators_[minipage_index - 1U];
      }
    } else {
      separator_low = minipage.separators_[pointer_index - 1U];
    }
    if (pointer_index == minipage.key_count_) {
      if (minipage_index == key_count) {
        separator_high = high_fence_;
      } else {
        separator_high = separators_[minipage_index];
      }
    } else {
      separator_high = minipage.separators_[pointer_index];
    }
    if (searching_slice < separator_low || searching_slice > separator_high) {
      VLOG(0) << "Interesting. there seems some change in this interior page. retry adoption";
      return kErrorCodeOk;
    }
  }

  if (minipage.key_count_ == kMaxIntermediateMiniSeparators) {
    // oh, then we also have to do rebalance
    // at this point we have to lock the whole page
    ASSERT_ND(key_count <= kMaxIntermediateSeparators);
    if (key_count == kMaxIntermediateSeparators) {
      // even that is impossible. let's split the whole page
      CHECK_ERROR_CODE(split_foster_and_adopt(context, child));
      return kErrorCodeOk;  // retry to re-calculate indexes. it's simpler
    }

    ASSERT_ND(key_count < kMaxIntermediateSeparators);
    // okay, it's possible to create a new first-level entry.
    // there are a few ways to do this.
    // 1) rebalance the whole page. in many cases this achieves the best layout for upcoming
    // inserts. so basically we do this.
    // 2) append to the end. this is very efficient if the inserts are sorted.
    // quite similar to the "no-record split" optimization in border page.
    if (key_count == minipage_index && minipage.key_count_ == pointer_index) {
      // this strongly suggests that it's a sorted insert. let's do that.
      adopt_from_child_norecord_first_level(minipage_index, child);
    } else {
      // in this case, we locally rebalance.
      CHECK_ERROR_CODE(local_rebalance(context));
    }
    return kErrorCodeOk;  // retry to re-calculate indexes
  }

  // okay, then most likely this is minipage-local. good
  uint8_t mini_key_count = minipage.key_count_;
  if (mini_key_count == kMaxIntermediateMiniSeparators) {
    VLOG(0) << "Interesting. concurrent inserts prevented adoption. retry";
    return kErrorCodeOk;  // retry
  }

  // now lock the child.
  child->lock();
  {
    UnlockScope scope_child(child);
    if (child->get_version().is_retired()) {
      VLOG(0) << "Interesting. concurrent inserts already adopted. retry";
      return kErrorCodeOk;  // retry
    }
    // this is guaranteed because these flag are immutable once set.
    ASSERT_ND(child->is_moved());
    ASSERT_ND(child->has_foster_child());
    ASSERT_ND(child->get_foster_minor());
    ASSERT_ND(child->get_foster_major());
    ASSERT_ND(!child->get_foster_minor()->header().snapshot_);
    ASSERT_ND(!child->get_foster_major()->header().snapshot_);
    // we adopt child's foster_major as a new pointer,
    // also adopt child's foster_minor as a replacement of child, making child retired.
    MasstreePage* grandchild_minor = child->get_foster_minor();
    ASSERT_ND(grandchild_minor->get_low_fence() == child->get_low_fence());
    ASSERT_ND(grandchild_minor->get_high_fence() == child->get_foster_fence());
    MasstreePage* grandchild_major = child->get_foster_major();
    ASSERT_ND(grandchild_major->get_low_fence() == child->get_foster_fence());
    ASSERT_ND(grandchild_major->get_high_fence() == child->get_high_fence());

    KeySlice new_separator = child->get_foster_fence();
    VolatilePagePointer minor_pointer;
    minor_pointer.word = grandchild_minor->header().page_id_;
    VolatilePagePointer major_pointer;
    major_pointer.word = grandchild_major->header().page_id_;

    // now we are sure we can adopt the child's foster twin.
    ASSERT_ND(pointer_index <= mini_key_count);
    ASSERT_ND(pointer_index == minipage.find_pointer(mini_key_count, searching_slice));
    if (pointer_index == mini_key_count) {
      // this means we are appending at the end. no need for split flag.
      DVLOG(1) << "Adopt without split. lucky. sequential inserts?";
    } else {
      // we have to shift elements.
      DVLOG(1) << "Adopt with splits.";
      std::memmove(
        minipage.separators_ + pointer_index + 1,
        minipage.separators_ + pointer_index,
        sizeof(KeySlice) * (mini_key_count - pointer_index));
      std::memmove(
        minipage.pointers_ + pointer_index + 2,
        minipage.pointers_ + pointer_index + 1,
        sizeof(DualPagePointer) * (mini_key_count - pointer_index));
    }

    ASSERT_ND(!minipage.pointers_[pointer_index].is_both_null());
    minipage.separators_[pointer_index] = new_separator;
    minipage.pointers_[pointer_index + 1].snapshot_pointer_ = 0;
    minipage.pointers_[pointer_index + 1].volatile_pointer_ = major_pointer;

    // we don't have to adopt the foster-minor because that's the child page itself,
    // but we have to switch the pointer
    minor_pointer.components.mod_count
      = minipage.pointers_[pointer_index].volatile_pointer_.components.mod_count + 1;
    minipage.pointers_[pointer_index].snapshot_pointer_ = 0;
    minipage.pointers_[pointer_index].volatile_pointer_ = minor_pointer;

    // we increase key count after above, with fence, so that concurrent transactions
    // never see an empty slot.
    assorted::memory_fence_release();
    ++minipage.key_count_;
    ASSERT_ND(minipage.key_count_ <= kMaxIntermediateMiniSeparators);
    ASSERT_ND(minipage.key_count_ == mini_key_count + 1);

    // the ex-child page now retires.
    child->get_version().set_retired();
    // TODO(Hideaki) it will be garbage-collected later.
    verify_separators();
  }

  return kErrorCodeOk;
}


void MasstreeIntermediatePage::adopt_from_child_norecord_first_level(
  uint8_t minipage_index,
  MasstreePage* child) {
  ASSERT_ND(is_locked());
  // note that we have to lock from parent to child. otherwise deadlock possible.
  MiniPage& minipage = get_minipage(minipage_index);
  child->lock();
  UnlockScope scope_child(child);
  if (child->get_version().is_retired()) {
    VLOG(0) << "Interesting. concurrent thread has already adopted? retry";
    return;
  }
  ASSERT_ND(child->is_moved());
  ASSERT_ND(child->has_foster_child());

  // in this case we don't need to increment split count.
  DVLOG(0) << "Great, sorted insert. No-split adopt";
  MasstreePage* grandchild_minor = child->get_foster_minor();
  ASSERT_ND(grandchild_minor->get_low_fence() == child->get_low_fence());
  ASSERT_ND(grandchild_minor->get_high_fence() == child->get_foster_fence());
  MasstreePage* grandchild_major = child->get_foster_major();
  ASSERT_ND(grandchild_major->get_low_fence() == child->get_foster_fence());
  ASSERT_ND(grandchild_major->get_high_fence() == child->get_high_fence());

  KeySlice new_separator = child->get_foster_fence();
  VolatilePagePointer minor_pointer;
  minor_pointer.word = grandchild_minor->header().page_id_;
  VolatilePagePointer major_pointer;
  major_pointer.word = grandchild_major->header().page_id_;

  MiniPage& new_minipage = mini_pages_[minipage_index + 1];
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
  minor_pointer.components.mod_count = old_pointer.volatile_pointer_.components.mod_count + 1;
  old_pointer.snapshot_pointer_ = 0;
  old_pointer.volatile_pointer_ = minor_pointer;
  // the ex-child page is now thrown away.
  // TODO(Hideaki) it will be garbage-collected later.
  child->get_version().set_retired();

  separators_[minipage_index] = new_separator;

  // increment key count after all with fence so that concurrent transactions never see
  // a minipage that is not ready for read
  assorted::memory_fence_release();
  get_version().increment_key_count();
  ASSERT_ND(get_version().get_key_count() == minipage_index + 1);
  verify_separators();
}

bool MasstreeBorderPage::track_moved_record(
  xct::XctId* owner_address,
  MasstreeBorderPage** located_page,
  uint8_t* located_index) {
  ASSERT_ND(is_moved());
  ASSERT_ND(has_foster_child());
  ASSERT_ND(get_foster_minor());
  ASSERT_ND(get_foster_major());
  ASSERT_ND(!header().snapshot_);
  ASSERT_ND(header().get_page_type() == kMasstreeBorderPageType);
  ASSERT_ND(owner_address >= owner_ids_);
  ASSERT_ND(owner_address - owner_ids_ < kMaxKeys);
  ASSERT_ND(owner_address - owner_ids_ < get_version().get_key_count());
  uint8_t index = owner_address - owner_ids_;
  KeySlice slice = slices_[index];
  uint8_t remaining = remaining_key_length_[index];
  bool originally_pointer = false;
  if (remaining == kKeyLengthNextLayer) {
    remaining = sizeof(KeySlice);
    originally_pointer = true;
  }
  const char* suffix = get_record(index);

  // recursively track. although probably it's only one level
  MasstreeBorderPage* cur_page = this;
  while (true) {
    if (cur_page->is_moved()) {
      ASSERT_ND(cur_page->has_foster_child());
      if (cur_page->within_foster_minor(slice)) {
        ASSERT_ND(!cur_page->within_foster_major(slice));
        cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page->get_foster_minor());
      } else {
        ASSERT_ND(cur_page->within_foster_major(slice));
        cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page->get_foster_major());
      }
      continue;
    }

    // now cur_page must be the page that contains the record.
    // the only exception is
    // 1) again the record is being moved concurrently
    // 2) the record was moved to another layer (remaining==kKeyLengthNextLayer).

    uint8_t keys = cur_page->get_version().get_key_count();
    *located_index = cur_page->find_key(keys, slice, suffix, remaining);
    if (*located_index == kMaxKeys) {
      // this can happen rarely because we are not doing the stable version trick here.
      // this is rare, so we just abort. no safety violation.
      VLOG(0) << "Very interesting. moved record not found due to concurrent updates";
      *located_index = cur_page->find_key(keys, slice, suffix, remaining);
      return false;
    } else if (cur_page->remaining_key_length_[*located_index] == kKeyLengthNextLayer &&
      !originally_pointer) {
      // another rare case. the record has been moved to another layer.
      // we can potentially track it, but not worth doing. abort.
      // TODO(Hideaki) we should track even in this case.
      VLOG(0) << "Interesting. moved record are now in another layer";
      return false;
    }

    // Otherwise, this is it!
    *located_page = cur_page;
    break;
  }
  return true;
}


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
