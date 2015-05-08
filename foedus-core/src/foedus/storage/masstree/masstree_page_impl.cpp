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
#include "foedus/storage/masstree/masstree_page_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/rdtsc_watch.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
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
  uint8_t             level,
  KeySlice            low_fence,
  KeySlice            high_fence) {
  // std::memset(this, 0, kPageSize);  // expensive
  header_.init_volatile(page_id, storage_id, page_type);
  header_.masstree_layer_ = layer;
  header_.masstree_in_layer_level_ = level;
  ASSERT_ND((page_type == kMasstreeIntermediatePageType) == (level > 0));
  high_fence_ = high_fence;
  low_fence_ = low_fence;
  foster_fence_ = low_fence;
  foster_twin_[0].word = 0;
  foster_twin_[1].word = 0;
  ASSERT_ND(get_key_count() == 0);
}

void MasstreePage::initialize_snapshot_common(
  StorageId           storage_id,
  SnapshotPagePointer page_id,
  PageType            page_type,
  uint8_t             layer,
  uint8_t             level,
  KeySlice            low_fence,
  KeySlice            high_fence) {
  header_.init_snapshot(page_id, storage_id, page_type);
  header_.masstree_layer_ = layer;
  header_.masstree_in_layer_level_ = level;
  ASSERT_ND((page_type == kMasstreeIntermediatePageType) == (level > 0));
  high_fence_ = high_fence;
  low_fence_ = low_fence;
  foster_fence_ = low_fence;
  foster_twin_[0].word = 0;
  foster_twin_[1].word = 0;
  ASSERT_ND(get_key_count() == 0);
}

void MasstreeIntermediatePage::initialize_volatile_page(
  StorageId           storage_id,
  VolatilePagePointer page_id,
  uint8_t             layer,
  uint8_t             level,
  KeySlice            low_fence,
  KeySlice            high_fence) {
  initialize_volatile_common(
    storage_id,
    page_id,
    kMasstreeIntermediatePageType,
    layer,
    level,
    low_fence,
    high_fence);
  for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    get_minipage(i).key_count_ = 0;
  }
}

void MasstreeIntermediatePage::initialize_snapshot_page(
  StorageId           storage_id,
  SnapshotPagePointer page_id,
  uint8_t             layer,
  uint8_t             level,
  KeySlice            low_fence,
  KeySlice            high_fence) {
  initialize_snapshot_common(
    storage_id,
    page_id,
    kMasstreeIntermediatePageType,
    layer,
    level,
    low_fence,
    high_fence);
  for (uint16_t i = 0; i <= kMaxIntermediateSeparators; ++i) {
    get_minipage(i).key_count_ = 0;
  }
}

void MasstreeBorderPage::initialize_volatile_page(
  StorageId           storage_id,
  VolatilePagePointer page_id,
  uint8_t             layer,
  KeySlice            low_fence,
  KeySlice            high_fence) {
  initialize_volatile_common(
    storage_id,
    page_id,
    kMasstreeBorderPageType,
    layer,
    0,
    low_fence,
    high_fence);
  consecutive_inserts_ = true;  // initially key_count = 0, so of course sorted
}

void MasstreeBorderPage::initialize_snapshot_page(
  StorageId           storage_id,
  SnapshotPagePointer page_id,
  uint8_t             layer,
  KeySlice            low_fence,
  KeySlice            high_fence) {
  initialize_snapshot_common(
    storage_id,
    page_id,
    kMasstreeBorderPageType,
    layer,
    0,
    low_fence,
    high_fence);
  consecutive_inserts_ = true;  // snapshot pages are always completely sorted
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


void release_parallel(Engine* engine, VolatilePagePointer pointer) {
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine->get_memory_manager()->get_global_volatile_page_resolver();
  MasstreePage* p = reinterpret_cast<MasstreePage*>(page_resolver.resolve_offset(pointer));
  memory::PageReleaseBatch release_batch(engine);
  p->release_pages_recursive_common(page_resolver, &release_batch);
  release_batch.release_all();
}


void MasstreeIntermediatePage::release_pages_recursive_parallel(Engine* engine) {
  // so far, we spawn a thread for every single pointer.
  // it might be an oversubscription, but not a big issue.
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    if (!foster_twin_[i].is_null()) {
      threads.emplace_back(release_parallel, engine, foster_twin_[i]);
    }
  }
  uint16_t key_count = get_key_count();
  ASSERT_ND(key_count <= kMaxIntermediateSeparators);
  for (uint8_t i = 0; i < key_count + 1; ++i) {
    MiniPage& minipage = get_minipage(i);
    uint16_t mini_count = minipage.key_count_;
    ASSERT_ND(mini_count <= kMaxIntermediateMiniSeparators);
    for (uint8_t j = 0; j < mini_count + 1; ++j) {
      VolatilePagePointer pointer = minipage.pointers_[j].volatile_pointer_;
      if (pointer.components.offset != 0) {
        threads.emplace_back(release_parallel, engine, pointer);
      }
    }
  }

  for (auto& t : threads) {
    t.join();
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  memory::PagePool* pool = engine->get_memory_manager()->get_node_memory(
    volatile_id.components.numa_node)->get_volatile_pool();
  pool->release_one(volatile_id.components.offset);
}

void MasstreeIntermediatePage::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  for (int i = 0; i < 2; ++i) {
    if (!foster_twin_[i].is_null()) {
      MasstreeIntermediatePage* p =
        reinterpret_cast<MasstreeIntermediatePage*>(page_resolver.resolve_offset(foster_twin_[i]));
      p->release_pages_recursive(page_resolver, batch);
      foster_twin_[i].word = 0;
    }
  }
  uint16_t key_count = get_key_count();
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
    if (!foster_twin_[i].is_null()) {
      MasstreeBorderPage* p
        = reinterpret_cast<MasstreeBorderPage*>(page_resolver.resolve_offset(foster_twin_[i]));
      p->release_pages_recursive(page_resolver, batch);
      foster_twin_[i].word = 0;
    }
  }
  uint16_t key_count = get_key_count();
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
  ASSERT_ND(get_key_count() == 0);
  ASSERT_ND(!is_locked());  // we don't lock a new page
  ASSERT_ND(copy_from->get_owner_id(copy_index)->is_keylocked());
  uint8_t parent_key_length = copy_from->remaining_key_length_[copy_index];
  ASSERT_ND(parent_key_length != kKeyLengthNextLayer);
  ASSERT_ND(parent_key_length > sizeof(KeySlice));
  uint8_t remaining = parent_key_length - sizeof(KeySlice);

  // retrieve the first 8 byte (or less) as the new slice.
  const char* parent_record = copy_from->get_record(copy_index);
  KeySlice new_slice = slice_key(parent_record, remaining);
  uint16_t payload_length = copy_from->payload_length_[copy_index];
  uint8_t suffix_length_aligned = calculate_suffix_length_aligned(remaining);

  slices_[0] = new_slice;
  remaining_key_length_[0] = remaining;
  payload_length_[0] = payload_length;
  offsets_[0] = (kDataSize - calculate_record_size(remaining, payload_length)) >> 4;
  consecutive_inserts_ = true;

  // use the same xct ID. This means we also inherit deleted flag.
  owner_ids_[0].xct_id_ = copy_from->owner_ids_[copy_index].xct_id_;
  // but we don't want to inherit locks
  owner_ids_[0].lock_.reset();
  if (suffix_length_aligned > 0) {
    // because suffix parts are 8-byte aligned with zero padding, we can memcpy in 8-bytes unit
    std::memcpy(get_record(0), parent_record + sizeof(KeySlice), suffix_length_aligned);
  }
  std::memcpy(
    get_record(0) + suffix_length_aligned,
    copy_from->get_record_payload(copy_index),
    assorted::align8(payload_length));  // payload is zero-padded to 8 bytes, too.

  // as we don't lock this page, we directly increment it to avoid is_locked assertion
  ++header_.key_count_;
}

inline ErrorCode grab_free_pages(
  thread::Thread* context,
  uint32_t count,
  memory::PagePoolOffset* offsets) {
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  for (uint32_t i = 0; i < count; ++i) {
    offsets[i] = memory->grab_free_volatile_page();
    if (offsets[i] == 0) {
      for (uint32_t j = 0; j < i; ++j) {
        memory->release_free_volatile_page(offsets[j]);
      }
      return kErrorCodeMemoryNoFreePages;
    }
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
  MasstreeBorderPage** target,
  xct::McsBlockIndex *target_lock) {
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(is_locked());
  ASSERT_ND(!is_moved());
  ASSERT_ND(foster_twin_[0].is_null() && foster_twin_[1].is_null());  // same as !is_moved()
  *target_lock = 0;
  debugging::RdtscWatch watch;

  uint8_t key_count = get_key_count();
  DVLOG(1) << "Splitting a page... ";

  memory::PagePoolOffset offsets[2];
  CHECK_ERROR_CODE(grab_free_pages(context, 2, offsets));

  // from now on no failure possible.
  BorderSplitStrategy strategy = split_foster_decide_strategy(key_count, trigger);
  MasstreeBorderPage* twin[2];
  xct::McsBlockIndex twin_locks[2];
  for (int i = 0; i < 2; ++i) {
    twin[i] = reinterpret_cast<MasstreeBorderPage*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(offsets[i]));
    foster_twin_[i].set(context->get_numa_node(), 0, 0, offsets[i]);
    VolatilePagePointer new_page_id
      = combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offsets[i]);
    twin[i]->initialize_volatile_page(
      header_.storage_id_,
      new_page_id,
      get_layer(),
      i == 0 ? low_fence_ : strategy.mid_slice_,  // low-fence
      i == 0 ? strategy.mid_slice_ : high_fence_);  // high-fence
    twin_locks[i] = context->mcs_initial_lock(twin[i]->get_lock_address());
    ASSERT_ND(twin[i]->is_locked());
  }
  xct::McsBlockIndex head_lock = split_foster_lock_existing_records(context, key_count);
  if (strategy.no_record_split_) {
    // in this case, we can move all records in one memcpy.
    // copy everything from the end of header to the end of page
    std::memcpy(
      &(twin[0]->remaining_key_length_),
      &(remaining_key_length_),
      kPageSize - sizeof(MasstreePage));
    for (uint8_t i = 0; i < key_count; ++i) {
      ASSERT_ND(twin[0]->owner_ids_[i].is_keylocked());
      twin[0]->owner_ids_[i].get_key_lock()->reset();  // no race
    }
    twin[0]->set_key_count(key_count);
    twin[1]->set_key_count(0);
    twin[0]->consecutive_inserts_ = consecutive_inserts_;
    twin[1]->consecutive_inserts_ = true;
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

  foster_fence_ = strategy.mid_slice_;
  assorted::memory_fence_release();

  // invoking set_moved is the point we announce all of these changes. take fence to make it right
  get_version().set_moved();
  assorted::memory_fence_release();

  // release all record locks, but set the "moved" bit so that concurrent transactions
  // check foster-twin for read-set/write-set checks.
  for (uint8_t i = 0; i < key_count; ++i) {
    xct::XctId new_id = owner_ids_[i].xct_id_;
    new_id.set_moved();
    owner_ids_[i].xct_id_ = new_id;
  }
  {
    // release locks in a batch
    xct::McsLock* mcs_locks[kMaxKeys];
    for (uint8_t i = 0; i < key_count; ++i) {
      mcs_locks[i] = owner_ids_[i].get_key_lock();
    }
    context->mcs_release_lock_batch(mcs_locks, head_lock, key_count);
  }

  assorted::memory_fence_release();

  // this page is now "moved".
  // which will be the target page?
  if (within_foster_minor(trigger)) {
    *target = twin[0];
    *target_lock = twin_locks[0];
    context->mcs_release_lock(twin[1]->get_lock_address(), twin_locks[1]);
  } else {
    ASSERT_ND(within_foster_major(trigger));
    *target = twin[1];
    *target_lock = twin_locks[1];
    context->mcs_release_lock(twin[0]->get_lock_address(), twin_locks[0]);
  }

  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to split a page. original page physical"
    << " record count: " << static_cast<int>(key_count)
    << "->" << get_key_count();
  return kErrorCodeOk;
}

BorderSplitStrategy MasstreeBorderPage::split_foster_decide_strategy(
  uint8_t key_count,
  KeySlice trigger) const {
  ASSERT_ND(key_count > 0);
  BorderSplitStrategy ret;
  ret.original_key_count_ = key_count;
  ret.no_record_split_ = false;
  ret.smallest_slice_ = slices_[0];
  ret.largest_slice_ = slices_[0];

  // if consecutive_inserts_, we are already sure about the key distributions, so easy.
  if (consecutive_inserts_) {
    ret.largest_slice_ = slices_[key_count - 1];
    if (trigger > ret.largest_slice_) {
      ret.no_record_split_ = true;
      DVLOG(1) << "Obviously no record split. key_count=" << static_cast<int>(key_count);
      ret.mid_slice_ = ret.largest_slice_ + 1;
      return ret;
    } else {
      DVLOG(1) << "Breaks a sequential page. key_count=" << static_cast<int>(key_count);
      ret.mid_slice_ = slices_[key_count / 2];
      return ret;
    }
  }

  for (uint8_t i = 1; i < key_count; ++i) {
    ret.smallest_slice_ = std::min<KeySlice>(slices_[i], ret.smallest_slice_);
    ret.largest_slice_ = std::max<KeySlice>(slices_[i], ret.largest_slice_);
  }

  ASSERT_ND(key_count >= 2U);  // because it's not consecutive, there must be at least 2 records.

  {
    // even if not, there is another easy case where two "tides" mix in this page;
    // one tide from left sequentially inserts keys while another tide from right also sequentially
    // inserts keys that are larger than left tide. This usually happens at the boundary of
    // two largely independent partitions (eg multiple threads inserting keys of their partition).
    // In that case, we should cleanly separate the two tides by picking the smallest key from
    // right-tide as the separator.
    KeySlice tides_max[2];
    KeySlice second_tide_min = kInfimumSlice;
    bool first_tide_broken = false;
    bool both_tides_broken = false;
    tides_max[0] = slices_[0];
    // for example, consider the following case:
    //   1 2 32 33 3 4 34 x
    // There are two tides 1- and 32-. We detect them as follows.
    // We initially consider 1,2,32,33 as the first tide because they are sequential.
    // Then, "3" breaks the first tide. We then consider 1- and 32- as the two tides.
    // If x breaks the tide again, we give up.
    for (uint8_t i = 1; i < key_count; ++i) {
      // look for "tide breaker" that is smaller than the max of the tide.
      // as soon as we found two of them (meaning 3 tides or more), we give up.
      KeySlice slice = slices_[i];
      if (!first_tide_broken)  {
        if (slice >= tides_max[0]) {
          tides_max[0] = slice;
          continue;  // ok!
        } else {
          // let's find where a second tide starts.
          first_tide_broken = true;
          uint8_t first_breaker;
          for (first_breaker = 0; first_breaker < i; ++first_breaker) {
            if (slices_[first_breaker] > slice) {
              break;
            }
          }
          ASSERT_ND(first_breaker < i);
          tides_max[0] = slice;
          ASSERT_ND(second_tide_min == kInfimumSlice);
          second_tide_min = slices_[first_breaker];
          tides_max[1] = slices_[i - 1];
          ASSERT_ND(tides_max[0] < tides_max[1]);
          ASSERT_ND(tides_max[0] < second_tide_min);
          ASSERT_ND(second_tide_min <= tides_max[1]);
        }
      } else {
        if (slice < second_tide_min && slice >= tides_max[0]) {
          tides_max[0] = slice;
          continue;  // fine, in the first tide
        } else if (slice >= tides_max[1]) {
          tides_max[1] = slice;  // okay, in the second tide
        } else {
          DVLOG(2) << "Oops, third tide. not the easy case";
          both_tides_broken = true;
          break;
        }
      }
    }

    ASSERT_ND(first_tide_broken);
    if (!both_tides_broken) {
      DVLOG(0) << "Yay, figured out two-tides meeting in a page.";
      ret.mid_slice_ = second_tide_min;
      return ret;
    }
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

xct::McsBlockIndex MasstreeBorderPage::split_foster_lock_existing_records(
  thread::Thread* context,
  uint8_t key_count) {
  debugging::RdtscWatch watch;  // check how expensive this is
  // lock in address order. so, no deadlock possible
  // we have to lock them whether the record is deleted or not. all physical records.
  xct::McsLock* mcs_locks[kMaxKeys];
  for (uint8_t i = 0; i < key_count; ++i) {
    mcs_locks[i] = owner_ids_[i].get_key_lock();
  }
  xct::McsBlockIndex head_lock_index = context->mcs_acquire_lock_batch(mcs_locks, key_count);
  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to lock all of "
    << static_cast<int>(key_count) << " records while splitting";
  if (watch.elapsed() > (1ULL << 26)) {
    // if we see this often, we have to optimize this somehow.
    LOG(WARNING) << "wait, wait, it costed " << watch.elapsed() << " cycles to lock all of "
      << static_cast<int>(key_count) << " records while splitting!! that's a lot! storage="
      << context->get_engine()->get_storage_manager()->get_name(header_.storage_id_)
      << ", thread ID=" << context->get_thread_id();
  }
  return head_lock_index;
}

void MasstreeBorderPage::split_foster_migrate_records(
  const MasstreeBorderPage& copy_from,
  uint8_t key_count,
  KeySlice inclusive_from,
  KeySlice inclusive_to) {
  ASSERT_ND(get_key_count() == 0);
  uint8_t migrated_count = 0;
  uint16_t unused_space = kDataSize;
  // utilize the fact that records grow backwards.
  // memcpy contiguous records as much as possible
  uint16_t contiguous_copy_size = 0;
  uint16_t contiguous_copy_to_begin = 0;
  uint16_t contiguous_copy_from_begin = 0;
  bool sofar_consecutive = true;
  for (uint8_t i = 0; i < key_count; ++i) {
    if (copy_from.slices_[i] >= inclusive_from && copy_from.slices_[i] <= inclusive_to) {
      // move this record.
      slices_[migrated_count] = copy_from.slices_[i];
      remaining_key_length_[migrated_count] = copy_from.remaining_key_length_[i];
      payload_length_[migrated_count] = copy_from.payload_length_[i];
      owner_ids_[migrated_count].xct_id_ = copy_from.owner_ids_[i].xct_id_;
      owner_ids_[migrated_count].lock_.reset();
      if (sofar_consecutive && migrated_count > 0 &&
          (slices_[migrated_count - 1] > slices_[migrated_count] ||
            (slices_[migrated_count - 1] == slices_[migrated_count] &&
            remaining_key_length_[migrated_count - 1] > remaining_key_length_[migrated_count]))) {
        sofar_consecutive = false;
      }

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

  set_key_count(migrated_count);
  consecutive_inserts_ = sofar_consecutive;
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
  ASSERT_ND(foster_twin_[0].is_null() && foster_twin_[1].is_null());  // same as !is_moved()
  debugging::RdtscWatch watch;

  PageVersionLockScope trigger_scope(context, trigger_child->get_version_address());
  if (trigger_child->is_retired()) {
    VLOG(0) << "Interesting. this child is now retired, so someone else has already adopted.";
    return kErrorCodeOk;  // fine. the goal is already achieved
  }

  uint8_t key_count = get_key_count();
  ASSERT_ND(key_count == kMaxIntermediateSeparators);
  DVLOG(1) << "Splitting an intermediate page... ";
  verify_separators();

  memory::PagePoolOffset offsets[3];
  CHECK_ERROR_CODE(grab_free_pages(context, 3, offsets));
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset work_offset = offsets[2];

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
  xct::McsBlockIndex twin_locks[2];
  for (int i = 0; i < 2; ++i) {
    twin[i] = reinterpret_cast<MasstreeIntermediatePage*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(offsets[i]));
    foster_twin_[i].set(context->get_numa_node(), 0, 0, offsets[i]);
    VolatilePagePointer new_pointer = combine_volatile_page_pointer(
      context->get_numa_node(), 0, 0, offsets[i]);

    twin[i]->initialize_volatile_page(
      header_.storage_id_,
      new_pointer,
      get_layer(),
      get_btree_level(),  // foster child has the same level as foster-parent
      i == 0 ? low_fence_ : new_foster_fence,
      i == 0 ? new_foster_fence : high_fence_);
    twin_locks[i] = context->mcs_initial_lock(twin[i]->get_lock_address());
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
    twin[0]->set_key_count(key_count);
    twin[1]->set_key_count(0);
    ASSERT_ND(new_foster_fence == trigger_child->get_foster_fence());

    // also adopt foster twin of trigger_child
    DualPagePointer& major_pointer = twin[1]->get_minipage(0).pointers_[0];
    major_pointer.snapshot_pointer_ = 0;
    major_pointer.volatile_pointer_ = trigger_child->get_foster_major();
    MiniPage& new_minipage = twin[0]->get_minipage(key_count);
    DualPagePointer& old_pointer = new_minipage.pointers_[new_minipage.key_count_];
    ASSERT_ND(context->resolve(old_pointer.volatile_pointer_)
      == reinterpret_cast<Page*>(trigger_child));
    old_pointer.snapshot_pointer_ = 0;
    old_pointer.volatile_pointer_ = trigger_child->get_foster_minor();

    ASSERT_ND(context->resolve(major_pointer.volatile_pointer_)
      == context->resolve(trigger_child->get_foster_major()));
    ASSERT_ND(context->resolve(old_pointer.volatile_pointer_)
      == context->resolve(trigger_child->get_foster_minor()));
  }

  for (int i = 0; i < 2; ++i) {
    context->mcs_release_lock(twin[i]->get_lock_address(), twin_locks[i]);
  }

  if (no_record_split) {
    // trigger_child is retired.
    trigger_scope.set_changed();
    trigger_child->set_retired();
    context->collect_retired_volatile_page(
      construct_volatile_page_pointer(trigger_child->header().page_id_));
  }

  foster_fence_ = new_foster_fence;
  assorted::memory_fence_release();
  // invoking set_moved is the point we announce all of these changes. take fence to make it right
  set_moved();

  watch.stop();
  DVLOG(1) << "Costed " << watch.elapsed() << " cycles to split a node. original node"
    << " key count: " << static_cast<int>(key_count)
    << "->" << get_key_count()
    << (no_record_split ? " no record split" : " usual split");
  memory->release_free_volatile_page(work_offset);

  verify_separators();
  return kErrorCodeOk;
}

ErrorCode MasstreeIntermediatePage::split_foster_no_adopt(thread::Thread* context) {
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(is_locked());
  ASSERT_ND(!is_moved());
  ASSERT_ND(foster_twin_[0].is_null() && foster_twin_[1].is_null());  // same as !is_moved()
  DVLOG(1) << "Splitting an intermediate page without adopt.. ";
  verify_separators();

  memory::PagePoolOffset offsets[3];
  CHECK_ERROR_CODE(grab_free_pages(context, 3, offsets));
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset work_offset = offsets[2];

  // from now on no failure possible.
  KeySlice new_foster_fence;
  IntermediateSplitStrategy* strategy = reinterpret_cast<IntermediateSplitStrategy*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(work_offset));
  ASSERT_ND(sizeof(IntermediateSplitStrategy) <= kPageSize);
  split_foster_decide_strategy(strategy);
  new_foster_fence = strategy->mid_separator_;  // the new separator is the low fence of new page

  MasstreeIntermediatePage* twin[2];
  xct::McsBlockIndex twin_locks[2];
  for (int i = 0; i < 2; ++i) {
    twin[i] = reinterpret_cast<MasstreeIntermediatePage*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(offsets[i]));
    foster_twin_[i].set(context->get_numa_node(), 0, 0, offsets[i]);
    VolatilePagePointer new_pointer = combine_volatile_page_pointer(
      context->get_numa_node(), 0, 0, offsets[i]);

    twin[i]->initialize_volatile_page(
      header_.storage_id_,
      new_pointer,
      get_layer(),
      get_btree_level(),  // foster child has the same level as foster-parent
      i == 0 ? low_fence_ : new_foster_fence,
      i == 0 ? new_foster_fence : high_fence_);
    twin_locks[i] = context->mcs_initial_lock(twin[i]->get_lock_address());
    ASSERT_ND(twin[i]->is_locked());
  }


  // reconstruct both old page and new page.
  // left : 0, 1, ... mid_index
  // right : mid_index + 1, +2, ... total_count - 1
  twin[0]->split_foster_migrate_records(*strategy, 0, strategy->mid_index_ + 1, new_foster_fence);
  twin[1]->split_foster_migrate_records(
    *strategy,
    strategy->mid_index_ + 1,
    strategy->total_separator_count_,
    high_fence_);

  for (int i = 0; i < 2; ++i) {
    context->mcs_release_lock(twin[i]->get_lock_address(), twin_locks[i]);
  }

  foster_fence_ = new_foster_fence;
  assorted::memory_fence_release();
  set_moved();

  memory->release_free_volatile_page(work_offset);

  verify_separators();
  return kErrorCodeOk;
}

void MasstreeIntermediatePage::split_foster_decide_strategy(IntermediateSplitStrategy* out) const {
  ASSERT_ND(is_locked());
  out->total_separator_count_ = 0;
  uint8_t key_count = get_key_count();
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
  ASSERT_ND(out->total_separator_count_ >= 2U);
  // left takes 0 to mid_index, right takes mid_index+1 to total-1, thus if we simply
  // mid=total/2, right takes less (think about this: total=20, mid=10. #left=11, #right=9).
  // We thus use mid=(total-1)/2.  total=20,mid=9,left=right=10. total=21,mid=10,left=11,right=10
  out->mid_index_ = (out->total_separator_count_ - 1U) / 2;
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
  header_.set_key_count(cur_mini);  // set key count after all
  ASSERT_ND(get_key_count() <= kMaxIntermediateSeparators);

  // the last separator is ignored because it's foster-fence/high-fence.
  ASSERT_ND(next_separator == expected_last_separator);

  verify_separators();
}

void MasstreeIntermediatePage::verify_separators() const {
#ifndef NDEBUG
  for (uint8_t i = 0; i <= get_key_count(); ++i) {
    KeySlice low, high;
    if (i < get_key_count()) {
      if (i > 0) {
        low = separators_[i - 1];
      } else {
        low = low_fence_;
      }
      high = separators_[i];
      ASSERT_ND(separators_[i] > low);
    } else {
      ASSERT_ND(i == get_key_count());
      if (i == 0) {
        low = low_fence_;
      } else {
        low = separators_[i - 1];
      }
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

  uint8_t key_count = get_key_count();
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
    << "->" << get_key_count()
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
  PageVersionLockScope scope(context, get_version_address());
  if (is_moved()) {
    VLOG(0) << "Interesting. concurrent thread has already split this node? retry";
    return kErrorCodeOk;
  }

  uint8_t key_count = get_key_count();
  MiniPage& minipage = get_minipage(minipage_index);
  ASSERT_ND(minipage.key_count_ <= kMaxIntermediateMiniSeparators);
  {
    if (minipage_index > key_count || pointer_index > minipage.key_count_) {
      VLOG(0) << "Interesting. there seems some change in this interior page. retry adoption";
      return kErrorCodeOk;
    }

    // TASK(Hideaki) let's make this a function.
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
    scope.set_changed();
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
      adopt_from_child_norecord_first_level(context, minipage_index, child);
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
  {
    PageVersionLockScope scope_child(context, child->get_version_address());
    if (child->get_version().is_retired()) {
      VLOG(0) << "Interesting. concurrent inserts already adopted. retry";
      return kErrorCodeOk;  // retry
    }
    // this is guaranteed because these flag are immutable once set.
    ASSERT_ND(child->is_moved());
    ASSERT_ND(child->has_foster_child());
    ASSERT_ND(!child->get_foster_minor().is_null());
    ASSERT_ND(!child->get_foster_major().is_null());
    // we adopt child's foster_major as a new pointer,
    // also adopt child's foster_minor as a replacement of child, making child retired.
    MasstreePage* grandchild_minor = context->resolve_cast<MasstreePage>(child->get_foster_minor());
    ASSERT_ND(grandchild_minor->get_low_fence() == child->get_low_fence());
    ASSERT_ND(grandchild_minor->get_high_fence() == child->get_foster_fence());
    MasstreePage* grandchild_major = context->resolve_cast<MasstreePage>(child->get_foster_major());
    ASSERT_ND(grandchild_major->get_low_fence() == child->get_foster_fence());
    ASSERT_ND(grandchild_major->get_high_fence() == child->get_high_fence());
    ASSERT_ND(!grandchild_minor->header().snapshot_);
    ASSERT_ND(!grandchild_major->header().snapshot_);

    KeySlice new_separator = child->get_foster_fence();
    VolatilePagePointer minor_pointer;
    minor_pointer.word = grandchild_minor->header().page_id_;
    VolatilePagePointer major_pointer;
    major_pointer.word = grandchild_major->header().page_id_;

    // now we are sure we can adopt the child's foster twin.
    ASSERT_ND(pointer_index <= mini_key_count);
    ASSERT_ND(pointer_index == minipage.find_pointer(searching_slice));
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
    scope_child.set_changed();
    child->set_retired();
    context->collect_retired_volatile_page(
      construct_volatile_page_pointer(child->header().page_id_));
    verify_separators();
  }

  return kErrorCodeOk;
}


void MasstreeIntermediatePage::adopt_from_child_norecord_first_level(
  thread::Thread* context,
  uint8_t minipage_index,
  MasstreePage* child) {
  ASSERT_ND(is_locked());
  // note that we have to lock from parent to child. otherwise deadlock possible.
  MiniPage& minipage = get_minipage(minipage_index);
  PageVersionLockScope scope_child(context, child->get_version_address());
  if (child->get_version().is_retired()) {
    VLOG(0) << "Interesting. concurrent thread has already adopted? retry";
    return;
  }
  ASSERT_ND(child->is_moved());
  ASSERT_ND(child->has_foster_child());

  DVLOG(0) << "Great, sorted insert. No-split adopt";
  scope_child.set_changed();
  MasstreePage* grandchild_minor
    = reinterpret_cast<MasstreePage*>(context->resolve(child->get_foster_minor()));
  ASSERT_ND(grandchild_minor->get_low_fence() == child->get_low_fence());
  ASSERT_ND(grandchild_minor->get_high_fence() == child->get_foster_fence());
  MasstreePage* grandchild_major
    = reinterpret_cast<MasstreePage*>(context->resolve(child->get_foster_major()));
  ASSERT_ND(grandchild_major->get_low_fence() == child->get_foster_fence());
  ASSERT_ND(grandchild_major->get_high_fence() == child->get_high_fence());

  KeySlice new_separator = child->get_foster_fence();
  VolatilePagePointer minor_pointer = child->get_foster_minor();
  VolatilePagePointer major_pointer = child->get_foster_major();

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
  // it will be garbage-collected later.
  child->get_version().set_retired();
  context->collect_retired_volatile_page(
    construct_volatile_page_pointer(child->header().page_id_));

  separators_[minipage_index] = new_separator;

  // increment key count after all with fence so that concurrent transactions never see
  // a minipage that is not ready for read
  assorted::memory_fence_release();
  increment_key_count();
  ASSERT_ND(get_key_count() == minipage_index + 1);
  verify_separators();
}

MasstreePage* MasstreePage::track_foster_child(
  KeySlice slice,
  const memory::GlobalVolatilePageResolver& resolver) {
  MasstreePage* cur_page = this;
  while (cur_page->is_moved()) {
    ASSERT_ND(cur_page->has_foster_child());
    if (cur_page->within_foster_minor(slice)) {
      ASSERT_ND(!cur_page->within_foster_major(slice));
      cur_page = reinterpret_cast<MasstreePage*>(
        resolver.resolve_offset(cur_page->get_foster_minor()));
    } else {
      ASSERT_ND(cur_page->within_foster_major(slice));
      cur_page = reinterpret_cast<MasstreePage*>(
        resolver.resolve_offset(cur_page->get_foster_major()));
    }
  }
  return cur_page;
}

xct::TrackMovedRecordResult MasstreeBorderPage::track_moved_record(
  Engine* engine,
  xct::LockableXctId* owner_address,
  xct::WriteXctAccess* write_set) {
  ASSERT_ND(!header().snapshot_);
  ASSERT_ND(header().get_page_type() == kMasstreeBorderPageType);
  ASSERT_ND(owner_address >= owner_ids_);
  ASSERT_ND(owner_address - owner_ids_ < kMaxKeys);
  ASSERT_ND(owner_address - owner_ids_ < get_key_count());

  if (owner_address->xct_id_.is_next_layer()) {
    return track_moved_record_next_layer(engine, owner_address, write_set);
  }

  // otherwise, we can track without key information within this layer.
  // the slice and key length is enough to identify the record.
  ASSERT_ND(is_moved());
  ASSERT_ND(has_foster_child());
  ASSERT_ND(!get_foster_minor().is_null());
  ASSERT_ND(!get_foster_major().is_null());
  uint8_t original_index = owner_address - owner_ids_;
  KeySlice slice = slices_[original_index];
  uint8_t remaining = remaining_key_length_[original_index];
  if (remaining == kKeyLengthNextLayer) {  // we just checked it above.. but possible
    LOG(INFO) << "A very rare event. the record has now become a next layer pointer";
    return xct::TrackMovedRecordResult();
  }
  const char* suffix = get_record(original_index);

  // if remaining <= 8 : this layer can have only one record that has this slice and this length.
  // if remaining > 8  : this layer has either the exact record, or it's now a next layer pointer.

  // recursively track. although probably it's only one level
  MasstreeBorderPage* cur_page = this;
  const memory::GlobalVolatilePageResolver& resolver
    = engine->get_memory_manager()->get_global_volatile_page_resolver();
  while (true) {
    cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page->track_foster_child(slice, resolver));

    // now cur_page must be the page that contains the record.
    // the only exception is
    // 1) again the record is being moved concurrently
    // 2) the record was moved to another layer (remaining==kKeyLengthNextLayer).
    uint8_t index = cur_page->find_key(slice, suffix, remaining);
    if (index == kMaxKeys) {
      // this can happen rarely because we are not doing the stable version trick here.
      // this is rare, so we just abort. no safety violation.
      VLOG(0) << "Very interesting. moved record not found due to concurrent updates";
      return xct::TrackMovedRecordResult();
    }

    xct::LockableXctId* new_owner_address = cur_page->get_owner_id(index);
    char* new_record_address = cur_page->get_record(index);
    if (cur_page->does_point_to_layer(index)) {
      // another rare case. the record has been now moved to another layer.

      if (remaining <= sizeof(KeySlice)) {
        // the record we are looking for can't be stored in next layer..
        VLOG(0) << "Wtf 2. moved record in next layer not found. Probably due to concurrent thread";
        return xct::TrackMovedRecordResult();
      }

      VLOG(0) << "Interesting. moved record are now in another layer. further track.";
      return cur_page->track_moved_record_next_layer(engine, new_owner_address, write_set);
    }

    // Otherwise, this is it!
    // be careful, we give get_record() as "payload". the word "payload" is a bit overused here.
    return xct::TrackMovedRecordResult(new_owner_address, new_record_address);
  }
}

xct::TrackMovedRecordResult MasstreeBorderPage::track_moved_record_next_layer(
  Engine* engine,
  xct::LockableXctId* owner_address,
  xct::WriteXctAccess* write_set) {
  ASSERT_ND(!header().snapshot_);
  ASSERT_ND(header().get_page_type() == kMasstreeBorderPageType);
  ASSERT_ND(owner_address >= owner_ids_);
  ASSERT_ND(owner_address - owner_ids_ < kMaxKeys);
  ASSERT_ND(owner_address - owner_ids_ < get_key_count());
  ASSERT_ND(owner_address->xct_id_.is_next_layer());

  // if the record went down to next layer, we need write_set (its log) to track the exact key.
  if (write_set == nullptr) {
    VLOG(0) << "A rare event. The record went down to next layer. We need write-set to track"
      << " this, but seems like this is a read-only access. Give up tracking.";
    return xct::TrackMovedRecordResult();  // which will result in abort
  }

  ASSERT_ND(write_set);
  ASSERT_ND(write_set->log_entry_);
  const MasstreeCommonLogType* log_entry
    = reinterpret_cast<const MasstreeCommonLogType*>(write_set->log_entry_);
  uint16_t key_length = log_entry->key_length_;
  const char* key = log_entry->get_key();

  uint8_t cur_layer = get_layer();
  uint8_t next_layer = cur_layer + 1U;
  KeySlice next_slice = slice_layer(key, key_length, next_layer);
  ASSERT_ND(key_length > sizeof(KeySlice) * (cur_layer + 1U));
  uint16_t next_remaining = key_length - (cur_layer + 1U) * sizeof(KeySlice);
  const char* next_suffix = key + (cur_layer + 1U) * sizeof(KeySlice);

  uint8_t original_index = owner_address - owner_ids_;
  ASSERT_ND(slice_layer(key, key_length, cur_layer) == slices_[original_index]);
  ASSERT_ND(does_point_to_layer(original_index));

  VolatilePagePointer root_pointer = get_next_layer(original_index)->volatile_pointer_;
  if (root_pointer.is_null()) {
    VLOG(0) << "Wtf. moved record in next layer not found. Probably due to concurrent thread";
    return xct::TrackMovedRecordResult();
  }

  const memory::GlobalVolatilePageResolver& resolver
    = engine->get_memory_manager()->get_global_volatile_page_resolver();
  MasstreePage* cur_page
    = reinterpret_cast<MasstreeBorderPage*>(resolver.resolve_offset(root_pointer));
  ASSERT_ND(cur_page->get_low_fence() == kInfimumSlice && cur_page->is_high_fence_supremum());
  ASSERT_ND(cur_page->get_layer() == next_layer);

  while (true) {
    cur_page = cur_page->track_foster_child(next_slice, resolver);
    ASSERT_ND(cur_page->get_layer() == next_layer);
    ASSERT_ND(cur_page->within_fences(next_slice));

    if (!cur_page->is_border()) {
      MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(cur_page);
      uint8_t index = casted->find_minipage(next_slice);
      MasstreeIntermediatePage::MiniPage& minipage = casted->get_minipage(index);
      uint8_t index_mini = minipage.find_pointer(next_slice);
      VolatilePagePointer pointer = minipage.pointers_[index_mini].volatile_pointer_;
      if (pointer.is_null()) {
        VLOG(0) << "Wtf 1. moved record in next layer not found. Probably due to concurrent thread";
        return xct::TrackMovedRecordResult();
      }
      cur_page = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(pointer));
      ASSERT_ND(cur_page->get_layer() == next_layer);
      ASSERT_ND(cur_page->within_fences(next_slice));
      continue;
    }

    // now cur_page must be the page that contains the record.
    // the only exception is
    // 1) again the record is being moved concurrently
    // 2) the record was moved to another layer (remaining==kKeyLengthNextLayer).
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(cur_page);
    ASSERT_ND(casted != this);
    uint8_t index = casted->find_key(next_slice, next_suffix, next_remaining);
    if (index == kMaxKeys) {
      VLOG(0) << "Very interesting. moved record not found due to concurrent updates";
      return xct::TrackMovedRecordResult();
    }

    xct::LockableXctId* new_owner_address = casted->get_owner_id(index);
    ASSERT_ND(new_owner_address != owner_address);
    char* new_record_address = casted->get_record(index);
    if (casted->does_point_to_layer(index)) {
      // the record has been now moved to yet another layer.
      if (next_remaining <= sizeof(KeySlice)) {
        // the record we are looking for can't be stored in next layer..
        VLOG(0) << "Wtf 2. moved record in next layer not found. Probably due to concurrent thread";
        return xct::TrackMovedRecordResult();
      }

      VLOG(0) << "Interesting. moved record are now in another layer. further track.";
      return casted->track_moved_record_next_layer(engine, new_owner_address, write_set);
    }

    // be careful, we give get_record() as "payload". the word "payload" is a bit overused here.
    return xct::TrackMovedRecordResult(new_owner_address, new_record_address);
  }
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
