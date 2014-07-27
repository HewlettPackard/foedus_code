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
  std::memset(this, 0, kPageSize);
  header_.init_volatile(page_id, storage_id, page_type, root_in_layer);
  uint64_t ver = (layer << kPageVersionLayerShifts);
  if (initially_locked) {
    ver |= kPageVersionLockedBit;
  }
  if (foster_child) {
    ver |= kPageVersionHasFosterChildBit;
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
  foster_fence_ = foster_fence;
  foster_child_ = foster_child;
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
  KeySlice            foster_fence,
  MasstreePage*       foster_child,
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
    foster_fence,
    foster_child,
    initially_locked);
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

void MasstreePage::clear_foster() {
  ASSERT_ND(is_locked());
  ASSERT_ND(has_foster_child());
  ASSERT_ND(get_foster_child());
  header_.page_version_.data_ &= ~(kPageVersionHasFosterChildBit | kPageVersionIsSupremumBit);
  foster_child_ = nullptr;
  if (header_.get_page_type() == kMasstreeBorderPageType) {
    // only border pages have foster-minor.
    reinterpret_cast<MasstreeBorderPage*>(this)->clear_foster_minor();
  }
  high_fence_ = foster_fence_;
  foster_fence_ = low_fence_;
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
  if (foster_minor_) {
    foster_minor_->release_pages_recursive(page_resolver, batch);
    foster_minor_ = nullptr;
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
  ASSERT_ND(foster_minor_ == nullptr);  // essentially same as !is_moved()
  // a bit convoluted pre-condition. we don't allow splitting a page which had a no-record split
  // until the page is adopted by the parent. Foster-child and foster-twin is tricky to co-exist.
  // Anyways, this never happens because the next traversal adopts it.
  ASSERT_ND((foster_child_ == nullptr && foster_minor_ == nullptr) ||
      (foster_child_ != nullptr && foster_minor_ != nullptr));
  debugging::RdtscWatch watch;

  uint8_t key_count = header_.page_version_.get_key_count();
  header_.page_version_.set_splitting();

  DVLOG(1) << "Splitting a page... ";

  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  memory::PagePoolOffset minor_offset = memory->grab_free_volatile_page();
  if (minor_offset == 0) {
    memory->release_free_volatile_page(offset);
    return kErrorCodeMemoryNoFreePages;
  }

  BorderSplitStrategy strategy = split_foster_decide_strategy(key_count, trigger);

  // from now on no failure possible.
  MasstreeBorderPage* new_foster_child = reinterpret_cast<MasstreeBorderPage*>(
    context->get_local_volatile_page_resolver().resolve_offset(offset));
  new_foster_child->initialize_volatile_page(
    header_.storage_id_,
    combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offset),
    get_layer(),
    false,
    strategy.mid_slice_,  // the new separator is the low fence of new page
    high_fence_,          // high fence is same as this page
    is_high_fence_supremum(),
    foster_fence_,  // inherit foster key and child
    foster_child_,
    true);  // yes, lock it
  ASSERT_ND(new_foster_child->is_locked());
  if (strategy.no_record_split_) {
    // this one is easy because we don't have to move anything. even no lock.
    // actually, nothing to do here!
    memory->release_free_volatile_page(minor_offset);  // we don't use the foster_minor page either
    *target = new_foster_child;
  } else {
    // otherwise, we have to lock all records.
    // we can instead lock only moving records, but in reality we are moving almost all records
    // because of compaction. so just do it all.
    split_foster_lock_existing_records(key_count);
    // commit the system transaction to get xct_id
    xct::XctId new_id = split_foster_commit_system_xct(context, strategy);

    // to foster a child, a parent must change, too.
    MasstreeBorderPage* minor_page = reinterpret_cast<MasstreeBorderPage*>(
      context->get_local_volatile_page_resolver().resolve_offset(minor_offset));
    VolatilePagePointer minor_pointer;
    minor_pointer = combine_volatile_page_pointer(context->get_numa_node(), 0, 0, minor_offset);
    minor_page->initialize_volatile_page(
      header_.storage_id_,
      minor_pointer,
      get_layer(),
      false,
      low_fence_,
      strategy.mid_slice_,
      false,
      low_fence_,  // no foster child inherited to minor
      nullptr,
      true);
    ASSERT_ND(minor_page->is_locked());

    // from now on, it's kind of "apply" phase
    foster_minor_ = minor_page;
    minor_page->split_foster_migrate_records(
      *this,
      key_count,
      strategy.smallest_slice_,
      strategy.mid_slice_ - 1);  // to make it inclusive
    new_foster_child->split_foster_migrate_records(
      *this,
      key_count,
      strategy.mid_slice_,
      strategy.largest_slice_);  // this is inclusive (to avoid supremum hassles)

    // release all record locks, but set the "moved" bit so that concurrent transactions
    // check foster-twin for read-set/write-set checks.
    new_id.set_moved();
    assorted::memory_fence_release();
    for (uint8_t i = 0; i < key_count; ++i) {
      owner_ids_[i] = new_id;  // unlock and also notify that the record has been moved
    }
    assorted::memory_fence_release();

    // this page is now "moved".
    get_version().set_moved();

    // which will be the target page?
    if (minor_page->within_fences(trigger)) {
      *target = minor_page;
      new_foster_child->unlock();
    } else {
      ASSERT_ND(new_foster_child->within_fences(trigger));
      *target = new_foster_child;
      minor_page->unlock();
    }
  }

  foster_child_ = new_foster_child;
  foster_fence_ = strategy.mid_slice_;
  if (!header_.page_version_.has_foster_child()) {
    header_.page_version_.set_has_foster_child(true);
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
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to lock all of "
    << static_cast<int>(key_count) << " records while splitting";
  if (watch.elapsed() > (1ULL << 26)) {
    // if we see this often, we have to optimize this somehow.
    LOG(WARNING) << "wait, wait, it costed " << watch.elapsed() << " cycles to lock all of "
      << static_cast<int>(key_count) << " records while splitting!! that's a lot!";
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
      std::memcpy(get_record(migrated_count), copy_from.get_record(i), record_length);
      ++migrated_count;
    }
  }
  header_.page_version_.set_key_count(migrated_count);
}

/////////////////////////////////////////////////////////////////////////////////////
///
///                      Interior node's Split
///
/////////////////////////////////////////////////////////////////////////////////////
void MasstreeIntermediatePage::init_lock_all_mini() {
  ASSERT_ND(is_locked());
  for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    get_minipage(i).mini_version_.data_ |= (
      kPageVersionLockedBit |
      kPageVersionInsertingBit |
      kPageVersionSplittingBit);
  }
}
void MasstreeIntermediatePage::init_unlock_all_mini() {
  for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    get_minipage(i).mini_version_.data_ &= ~(
      kPageVersionLockedBit |
      kPageVersionInsertingBit |
      kPageVersionSplittingBit);
  }
}

ErrorCode MasstreeIntermediatePage::split_foster(
  thread::Thread* context,
  MasstreePage* trigger_child) {
  // similar to border page's split, but simpler in a few places because
  // 1) intermediate page doesn't have owner_id for each pointer (no lock concerns).
  // 2) intermediate page is already completed sorted.
  // thus, this is just a physical operation without any transactional behavior.
  // even not a system transaction
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(is_locked());
  debugging::RdtscWatch watch;

  uint8_t key_count = header_.page_version_.get_key_count();
  get_version().set_inserting();
  get_version().set_splitting();
  DVLOG(1) << "Splitting an intermediate page... ";
  verify_separators();

  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  memory::PagePoolOffset work_offset = memory->grab_free_volatile_page();
  if (work_offset == 0) {
    memory->release_free_volatile_page(offset);
    return kErrorCodeMemoryNoFreePages;
  }

  // from now on no failure possible.

  MasstreeIntermediatePage* new_page = reinterpret_cast<MasstreeIntermediatePage*>(
  context->get_local_volatile_page_resolver().resolve_offset(offset));
  VolatilePagePointer new_pointer;
  new_pointer = combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offset);
  KeySlice old_foster_fence = foster_fence_;

  // it might be a sorted insert.
  KeySlice new_foster_fence;
  bool no_record_split = false;
  const MiniPage& last_minipage = get_minipage(key_count);
  if (trigger_child->get_foster_fence()
      > last_minipage.separators_[last_minipage.mini_version_.get_key_count() - 1]) {
    DVLOG(0) << "Seems like a sequential insert. let's try no-record split";
    trigger_child->lock(true, true);
    // we are sure only after we lock it
    if (trigger_child->has_foster_child() && trigger_child->get_foster_fence()
        > last_minipage.separators_[last_minipage.mini_version_.get_key_count() - 1]) {
      DVLOG(0) << "Yes, we can do no-record split!";
      no_record_split = true;
      // triggering key as new separator (remember, low-fence is inclusive)
      new_foster_fence = trigger_child->get_foster_fence();
      // keep trigger_child locked. we have to unlock it later.
    } else {
      // unluckily we couldn't. fall back to usual path
      LOG(INFO) << "Interesting no-record split failed because of concurrent threads.";
      trigger_child->unlock();
    }
  }

  IntermediateSplitStrategy* strategy = nullptr;
  if (!no_record_split) {
    for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
      get_minipage(i).mini_version_.lock_version(true, true);
    }
    strategy = reinterpret_cast<IntermediateSplitStrategy*>(
        context->get_local_volatile_page_resolver().resolve_offset(work_offset));
    ASSERT_ND(sizeof(IntermediateSplitStrategy) <= kPageSize);
    split_foster_decide_strategy(strategy);
    new_foster_fence = strategy->mid_separator_;  // the new separator is the low fence of new page
  }

  new_page->initialize_volatile_page(
    header_.storage_id_,
    new_pointer,
    get_layer(),
    false,
    new_foster_fence,
    high_fence_,          // high fence is same as this page
    is_high_fence_supremum(),
    old_foster_fence,  // inherit foster key and child
    foster_child_,
    true);  // yes, lock it
  ASSERT_ND(new_page->is_locked());
  UnlockScope new_scope(new_page);
  new_page->init_lock_all_mini();

  if (!no_record_split) {
    // reconstruct both old page and new page.
    // we are copying contents from the strategy object, so no worry on overwritten source.
    this->split_foster_migrate_records(*strategy, 0, strategy->mid_index_ + 1, new_foster_fence);
    new_page->split_foster_migrate_records(
      *strategy,
      strategy->mid_index_ + 1,
      strategy->total_separator_count_,
      high_fence_);

    for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
      get_minipage(i).mini_version_.unlock_version();
    }
  } else {
    // no record split. this is it!
    new_page->get_version().set_key_count(0);
    new_page->get_minipage(0).mini_version_.set_key_count(0);
    new_page->get_minipage(0).pointers_[0].snapshot_pointer_ = 0;
    new_page->get_minipage(0).pointers_[0].volatile_pointer_.word
      = trigger_child->get_foster_child()->header().page_id_;
    new_page->get_minipage(0).pointers_[0].volatile_pointer_.components.flags = 0;
    trigger_child->clear_foster();
    trigger_child->unlock();
  }

  new_page->init_unlock_all_mini();
  foster_child_ = new_page;
  foster_fence_ = new_foster_fence;
  if (!header_.page_version_.has_foster_child()) {
    header_.page_version_.set_has_foster_child(true);
  }

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
    ASSERT_ND(mini_page.mini_version_.is_locked());
    uint8_t separator_count = mini_page.mini_version_.get_key_count();
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

#ifndef NDEBUG
  // for ease of debugging zero-out the page first (only data part). only for debug build.
  for (uint8_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    if (i < kMaxIntermediateSeparators) {
      separators_[i] = 0;
    }
    MiniPage& minipage = get_minipage(i);
    minipage.mini_version_.set_key_count(0);
    for (uint8_t j = 0; j <= kMaxIntermediateMiniSeparators; ++j) {
      if (j < kMaxIntermediateMiniSeparators) {
        minipage.separators_[j] = 0;
      }
      minipage.pointers_[j].snapshot_pointer_ = 0;
      minipage.pointers_[j].volatile_pointer_.word = 0;
    }
  }
#endif  // NDEBUG

  // construct this page. copy the separators and pointers.
  // we distribute them as much as possible in first level. if mini pages have little
  // entries to start with, following adoption would be only local.
  float entries_per_mini = static_cast<float>(strategy.mid_index_)
    / (kMaxIntermediateSeparators + 1);
  ASSERT_ND(to > from);
  const uint16_t move_count = to - from;

  // it looks a bit complicated because each separator is "one-off" due to first-level separator.
  // so we buffer one separator.
  float next_mini_threshold = entries_per_mini;
  uint8_t cur_mini = 0;
  uint8_t cur_mini_separators = 0;
  MiniPage* cur_mini_page = &get_minipage(0);
  ASSERT_ND(cur_mini_page->mini_version_.is_locked());
  cur_mini_page->pointers_[0] = strategy.pointers_[from];
  ASSERT_ND(!strategy.pointers_[from].is_both_null());
  KeySlice next_separator = strategy.separators_[from];

  for (uint16_t i = 1; i < move_count; ++i) {
    uint16_t original_index = i + from;
    ASSERT_ND(!strategy.pointers_[original_index].is_both_null());
    if (i >= next_mini_threshold && cur_mini < kMaxIntermediateSeparators) {
      // switch to next mini page. so, the separator goes to the first level
      cur_mini_page->mini_version_.set_key_count(cur_mini_separators);  // close the current
      ASSERT_ND(cur_mini_page->mini_version_.get_key_count() <= kMaxIntermediateMiniSeparators);

      separators_[cur_mini] = next_separator;

      next_mini_threshold += entries_per_mini;
      cur_mini_separators = 0;
      ++cur_mini;
      cur_mini_page = &get_minipage(cur_mini);
      ASSERT_ND(cur_mini_page->mini_version_.is_locked());

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
  cur_mini_page->mini_version_.set_key_count(cur_mini_separators);  // close the last one
  ASSERT_ND(cur_mini_page->mini_version_.get_key_count() <= kMaxIntermediateMiniSeparators);
  get_version().set_key_count(cur_mini);
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
    for (uint8_t j = 0; j <= minipage.mini_version_.get_key_count(); ++j) {
      ASSERT_ND(!minipage.pointers_[j].is_both_null());
      if (j < minipage.mini_version_.get_key_count()) {
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
  for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    get_minipage(i).mini_version_.lock_version(true, true);
  }

  // reuse the code of split.
  IntermediateSplitStrategy* strategy =
    reinterpret_cast<IntermediateSplitStrategy*>(
      context->get_local_volatile_page_resolver().resolve_offset(work_offset));
  split_foster_decide_strategy(strategy);

  // reconstruct this page.
  uint16_t count = strategy->total_separator_count_;
  split_foster_migrate_records(*strategy, 0, count, high_fence_);

  for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    get_minipage(i).mini_version_.unlock_version();
  }

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
  PageVersion cur_stable,
  uint8_t minipage_index,
  PageVersion mini_stable,
  uint8_t pointer_index,
  MasstreePage* child) {
  ASSERT_ND(mini_stable.get_key_count() <= kMaxIntermediateMiniSeparators);
  if (mini_stable.get_key_count() == kMaxIntermediateMiniSeparators) {
    // oh, then we also have to do rebalance
    // at this point we have to lock the whole page
    ASSERT_ND(cur_stable.get_key_count() <= kMaxIntermediateSeparators);
    lock();
    UnlockScope scope(this);
    if (get_version().get_split_counter() != cur_stable.get_split_counter()) {
      LOG(INFO) << "Interesting. concurrent thread has already split the entire node? retry";
      return kErrorCodeOk;
    } else if (get_version().get_key_count() == kMaxIntermediateSeparators) {
      // even that is impossible. let's split the whole page
      get_version().set_splitting();
      CHECK_ERROR_CODE(split_foster(context, child));
      return kErrorCodeOk;  // retry to re-calculate indexes. it's simpler
    }

    ASSERT_ND(get_version().get_key_count() < kMaxIntermediateSeparators);
    // okay, it's possible to create a new first-level entry.
    // there are a few ways to do this.
    // 1) rebalance the whole page. in many cases this achieves the best layout for upcoming
    // inserts. so basically we do this.
    // 2) append to the end. this is very efficient if the inserts are sorted.
    // quite similar to the "no-record split" optimization in border page.
    if (get_version().get_key_count() == minipage_index &&
        mini_stable.get_key_count() == pointer_index) {
      // this strongly suggests that it's a sorted insert. let's do that.
      adopt_from_child_norecord_first_level(minipage_index, mini_stable, child);
    } else {
      // in this case, we locally rebalance.
      get_version().set_splitting();
      CHECK_ERROR_CODE(local_rebalance(context));
    }
    return kErrorCodeOk;  // retry to re-calculate indexes
  }

  // okay, then most likely this is minipage-local. good
  MiniPage& minipage = get_minipage(minipage_index);
  minipage.mini_version_.lock_version();
  UnlockVersionScope mini_scope(&minipage.mini_version_);
  uint8_t mini_key_count = minipage.mini_version_.get_key_count();
  if (mini_key_count == kMaxIntermediateMiniSeparators ||
    minipage.mini_version_.get_split_counter() != mini_stable.get_split_counter()) {
    LOG(INFO) << "Interesting. concurrent inserts prevented adoption. retry";
    return kErrorCodeOk;  // retry
  }

  // now lock the child.
  child->lock(true, true);  // TODO(Hideaki) no need for changing split counter?
  {
    UnlockScope scope_child(child);
    if (!child->has_foster_child()) {
      LOG(INFO) << "Interesting. concurrent inserts already adopted. retry";
      return kErrorCodeOk;  // retry
    }
    ASSERT_ND(child->get_foster_child());
    ASSERT_ND(!child->get_foster_child()->header().snapshot_);
    MasstreePage* grandchild = child->get_foster_child();
    ASSERT_ND(grandchild->get_low_fence() == child->get_foster_fence());
    ASSERT_ND(grandchild->get_high_fence() == child->get_high_fence());

    KeySlice new_separator = child->get_foster_fence();
    VolatilePagePointer new_pointer;
    new_pointer.word = grandchild->header().page_id_;

    // now we are sure we can adopt the child's foster child.
    ASSERT_ND(pointer_index <= mini_key_count);
    ASSERT_ND(pointer_index == minipage.find_pointer(mini_key_count, searching_slice));
    if (pointer_index == mini_key_count) {
      // this means we are appending at the end. no need for split flag.
      DVLOG(1) << "Adopt without split. lucky. sequential inserts?";
    } else {
      // we have to shift elements.
      DVLOG(1) << "Adopt with splits.";
      minipage.mini_version_.set_splitting();
      std::memmove(
        minipage.separators_ + pointer_index + 1,
        minipage.separators_ + pointer_index,
        sizeof(KeySlice) * (mini_key_count - pointer_index));
      std::memmove(
        minipage.pointers_ + pointer_index + 2,
        minipage.pointers_ + pointer_index + 1,
        sizeof(DualPagePointer) * (mini_key_count - pointer_index));
    }

    minipage.mini_version_.set_inserting_and_increment_key_count();
    ASSERT_ND(minipage.mini_version_.get_key_count() == mini_key_count + 1);
    ASSERT_ND(!minipage.pointers_[pointer_index].is_both_null());
    minipage.separators_[pointer_index] = new_separator;
    minipage.pointers_[pointer_index + 1].snapshot_pointer_ = 0;
    minipage.pointers_[pointer_index + 1].volatile_pointer_ = new_pointer;

    // if the child is a border page, it has foster-twin rather than a foster child.
    // we don't have to adopt the foster-minor because that's the child page itself,
    // but we have to switch the pointer
    if (child->header().get_page_type() == foedus::storage::kMasstreeBorderPageType) {
      MasstreeBorderPage* foster_minor
        = reinterpret_cast<MasstreeBorderPage*>(child)->get_foster_minor();
      if (foster_minor) {
        VolatilePagePointer minor_pointer;
        minor_pointer.word = foster_minor->header().page_id_;
        minor_pointer.components.mod_count
          = minipage.pointers_[pointer_index].volatile_pointer_.components.mod_count + 1;
        minipage.pointers_[pointer_index].snapshot_pointer_ = 0;
        minipage.pointers_[pointer_index].volatile_pointer_ = minor_pointer;
        // the ex-child page is now thrown away.
        // TODO(Hideaki) it will be garbage-collected later.
        ASSERT_ND(foster_minor->get_low_fence() == child->get_low_fence());
        ASSERT_ND(foster_minor->get_high_fence() == new_separator);
        ASSERT_ND(foster_minor->get_high_fence() == grandchild->get_low_fence());
      } else {
        child->clear_foster();
        ASSERT_ND(child->get_high_fence() == new_separator);
        ASSERT_ND(child->get_high_fence() == grandchild->get_low_fence());
      }
    } else {
      child->clear_foster();
      ASSERT_ND(child->get_high_fence() == new_separator);
      ASSERT_ND(child->get_high_fence() == grandchild->get_low_fence());
    }
    verify_separators();
  }

  return kErrorCodeOk;
}


void MasstreeIntermediatePage::adopt_from_child_norecord_first_level(
  uint8_t minipage_index,
  PageVersion mini_stable,
  MasstreePage* child) {
  ASSERT_ND(is_locked());
  // note that we have to lock from parent to child. otherwise deadlock possible.
  MiniPage& minipage = get_minipage(minipage_index);
  minipage.mini_version_.lock_version();
  UnlockVersionScope mini_scope(&minipage.mini_version_);
  if (minipage.mini_version_.get_split_counter() != mini_stable.get_split_counter()) {
    LOG(INFO) << "Interesting. concurrent thread has already split the minipage? retry";
    return;
  }
  ASSERT_ND(minipage.mini_version_.get_key_count() == mini_stable.get_key_count());
  child->lock(true, true);
  UnlockScope scope_child(child);
  if (!child->has_foster_child()) {
    LOG(INFO) << "Interesting. concurrent thread has already adopted? retry";
    return;
  }

  // in this case we don't need to increment split count.
  DVLOG(0) << "Great, sorted insert. No-split adopt";
  get_version().set_inserting();

  MasstreePage* grandchild = child->get_foster_child();
  ASSERT_ND(grandchild->get_low_fence() == child->get_foster_fence());
  ASSERT_ND(grandchild->get_high_fence() == child->get_high_fence());
  KeySlice new_separator = child->get_foster_fence();
  VolatilePagePointer new_pointer;
  new_pointer.word = grandchild->header().page_id_;
  MiniPage& new_minipage = mini_pages_[minipage_index + 1];
  new_minipage.mini_version_.lock_version(true, true);
  UnlockVersionScope new_mini_scope(&(new_minipage.mini_version_));

#ifndef NDEBUG
  // for ease of debugging zero-out the page first (only data part). only for debug build.
  new_minipage.mini_version_.set_key_count(0);
  for (uint8_t j = 0; j <= kMaxIntermediateMiniSeparators; ++j) {
    if (j < kMaxIntermediateMiniSeparators) {
      new_minipage.separators_[j] = 0;
    }
    new_minipage.pointers_[j].snapshot_pointer_ = 0;
    new_minipage.pointers_[j].volatile_pointer_.word = 0;
  }
#endif  // NDEBUG

  new_minipage.mini_version_.set_key_count(0);
  new_minipage.pointers_[0].snapshot_pointer_ = 0;
  new_minipage.pointers_[0].volatile_pointer_ = new_pointer;

  // also handle foster-twin if it's border page
  if (child->header().get_page_type() == foedus::storage::kMasstreeBorderPageType) {
    MasstreeBorderPage* foster_minor
      = reinterpret_cast<MasstreeBorderPage*>(child)->get_foster_minor();
    if (foster_minor) {
      VolatilePagePointer minor_pointer;
      DualPagePointer& old_pointer = minipage.pointers_[mini_stable.get_key_count()];
      ASSERT_ND(static_cast<uint32_t>(child->header().page_id_)
        == old_pointer.volatile_pointer_.components.offset);
      minor_pointer.word = foster_minor->header().page_id_;
      minor_pointer.components.mod_count = old_pointer.volatile_pointer_.components.mod_count + 1;
      old_pointer.snapshot_pointer_ = 0;
      old_pointer.volatile_pointer_ = minor_pointer;
      // the ex-child page is now thrown away.
      // TODO(Hideaki) it will be garbage-collected later.
      ASSERT_ND(foster_minor->get_low_fence() == child->get_low_fence());
      ASSERT_ND(foster_minor->get_high_fence() == new_separator);
      ASSERT_ND(foster_minor->get_high_fence() == grandchild->get_low_fence());
    } else {
      child->clear_foster();
      ASSERT_ND(child->get_high_fence() == new_separator);
      ASSERT_ND(child->get_high_fence() == grandchild->get_low_fence());
    }
  } else {
    child->clear_foster();
    ASSERT_ND(child->get_high_fence() == new_separator);
    ASSERT_ND(child->get_high_fence() == grandchild->get_low_fence());
  }

  separators_[minipage_index] = new_separator;
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
  ASSERT_ND(foster_minor_);
  ASSERT_ND(foster_child_);
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
      if (cur_page->foster_minor_->within_fences(slice)) {
        ASSERT_ND(!cur_page->foster_child_->within_fences(slice));
        cur_page = cur_page->foster_minor_;
      } else {
        ASSERT_ND(cur_page->foster_child_->within_fences(slice));
        cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page->foster_child_);
      }
      continue;
    } else if (cur_page->has_foster_child() && cur_page->within_foster_child(slice)) {
      cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page->foster_child_);
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
      LOG(INFO) << "Very interesting. moved record not found due to concurrent updates";
      *located_index = cur_page->find_key(keys, slice, suffix, remaining);
      return false;
    } else if (cur_page->remaining_key_length_[*located_index] == kKeyLengthNextLayer &&
      !originally_pointer) {
      // another rare case. the record has been moved to another layer.
      // we can potentially track it, but not worth doing. abort.
      LOG(INFO) << "Very interesting. moved record are now in another layer";
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
