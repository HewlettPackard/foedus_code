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
#include "foedus/xct/xct_access.hpp"
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
  next_offset_ = 0;  // well, already implicitly zero-ed, but to be clear
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
  next_offset_ = 0;  // well, already implicitly zero-ed, but to be clear
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
  if (header_.page_version_.is_moved()) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_ND(!foster_twin_[i].is_null());
      threads.emplace_back(release_parallel, engine, foster_twin_[i]);
    }
    // if this page is moved, following pointers in this page results in double-release.
    // we just follow pointers in non-moved page.
  } else {
    uint16_t key_count = get_key_count();
    ASSERT_ND(key_count <= kMaxIntermediateSeparators);
    for (uint8_t i = 0; i < key_count + 1; ++i) {
      MiniPage& minipage = get_minipage(i);
      uint16_t mini_count = minipage.key_count_;
      ASSERT_ND(mini_count <= kMaxIntermediateMiniSeparators);
      for (uint8_t j = 0; j < mini_count + 1; ++j) {
        VolatilePagePointer pointer = minipage.pointers_[j].volatile_pointer_;
        if (!pointer.is_null()) {
          threads.emplace_back(release_parallel, engine, pointer);
        }
      }
    }
  }

  for (auto& t : threads) {
    t.join();
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  memory::PagePool* pool = engine->get_memory_manager()->get_node_memory(
    volatile_id.get_numa_node())->get_volatile_pool();
  pool->release_one(volatile_id.get_offset());
}

void MasstreeIntermediatePage::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  if (!is_empty_range()) {
    if (header_.page_version_.is_moved()) {
      for (int i = 0; i < 2; ++i) {
        ASSERT_ND(!foster_twin_[i].is_null());
        MasstreeIntermediatePage* p =
          reinterpret_cast<MasstreeIntermediatePage*>(
            page_resolver.resolve_offset(foster_twin_[i]));
        p->release_pages_recursive(page_resolver, batch);
        foster_twin_[i].word = 0;
      }
    } else {
      uint16_t key_count = get_key_count();
      ASSERT_ND(key_count <= kMaxIntermediateSeparators);
      for (uint8_t i = 0; i < key_count + 1; ++i) {
        MiniPage& minipage = get_minipage(i);
        uint16_t mini_count = minipage.key_count_;
        ASSERT_ND(mini_count <= kMaxIntermediateMiniSeparators);
        for (uint8_t j = 0; j < mini_count + 1; ++j) {
          VolatilePagePointer pointer = minipage.pointers_[j].volatile_pointer_;
          if (!pointer.is_null()) {
            MasstreePage* child = reinterpret_cast<MasstreePage*>(
              page_resolver.resolve_offset(pointer));
            child->release_pages_recursive_common(page_resolver, batch);
          }
        }
      }
    }
  } else {
    ASSERT_ND(!is_moved());
    ASSERT_ND(get_key_count() == 0);
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  batch->release(volatile_id);
}

void MasstreeBorderPage::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  if (header_.page_version_.is_moved()) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_ND(!foster_twin_[i].is_null());
      MasstreeBorderPage* p
        = reinterpret_cast<MasstreeBorderPage*>(page_resolver.resolve_offset(foster_twin_[i]));
      p->release_pages_recursive(page_resolver, batch);
      foster_twin_[i].word = 0;
    }
  } else {
    SlotIndex key_count = get_key_count();
    ASSERT_ND(key_count <= kBorderPageMaxSlots);
    for (SlotIndex i = 0; i < key_count; ++i) {
      if (does_point_to_layer(i)) {
        DualPagePointer* pointer = get_next_layer(i);
        if (!pointer->volatile_pointer_.is_null()) {
          MasstreePage* child = reinterpret_cast<MasstreePage*>(
            page_resolver.resolve_offset(pointer->volatile_pointer_));
          child->release_pages_recursive_common(page_resolver, batch);
        }
      }
    }
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  batch->release(volatile_id);
}


void MasstreeBorderPage::initialize_layer_root(
  const MasstreeBorderPage* copy_from,
  SlotIndex copy_index) {
  ASSERT_ND(get_key_count() == 0);
  ASSERT_ND(!is_locked());  // we don't lock a new page
  ASSERT_ND(copy_from->get_owner_id(copy_index)->is_keylocked());

  // This is a new page, so no worry on race.
  const SlotIndex new_index = 0;
  Slot* slot = get_new_slot(new_index);
  const Slot* parent_slot = copy_from->get_slot(copy_index);
  const SlotLengthPart parent_lengthes = parent_slot->lengthes_.components;
  const char* parent_record = copy_from->get_record_from_offset(parent_lengthes.offset_);

  KeyLength parent_remainder = parent_slot->remainder_length_;
  ASSERT_ND(parent_remainder != kInitiallyNextLayer);
  ASSERT_ND(parent_remainder > sizeof(KeySlice));
  KeyLength remainder = parent_remainder - sizeof(KeySlice);

  // retrieve the first 8 byte (or less) as the new slice.
  const KeySlice new_slice = slice_key(parent_record, parent_remainder);
  const PayloadLength payload_length = parent_lengthes.payload_length_;
  const KeyLength suffix_length_aligned = calculate_suffix_length_aligned(remainder);
  const DataOffset record_size = to_record_length(remainder, payload_length);
  const DataOffset new_offset = next_offset_;

  set_slice(new_index, new_slice);

  ASSERT_ND(next_offset_ == 0);
  slot->lengthes_.components.offset_ = new_offset;
  slot->lengthes_.components.unused_ = 0;
  slot->lengthes_.components.physical_record_length_ = record_size;
  slot->lengthes_.components.payload_length_ = payload_length;
  slot->original_physical_record_length_ = record_size;
  slot->remainder_length_ = remainder;
  slot->original_offset_ = new_offset;
  next_offset_ += record_size;
  consecutive_inserts_ = true;

  // use the same xct ID. This means we also inherit deleted flag.
  slot->tid_.xct_id_ = parent_slot->tid_.xct_id_;
  ASSERT_ND(!slot->tid_.xct_id_.is_next_layer());
  // but we don't want to inherit locks
  slot->tid_.lock_.reset();
  if (suffix_length_aligned > 0) {
    // because suffix parts are 8-byte aligned with zero padding, we can memcpy in 8-bytes unit
    std::memcpy(
      get_record_from_offset(new_offset),
      parent_record + sizeof(KeySlice),
      suffix_length_aligned);
  }
  if (payload_length > 0) {
    std::memcpy(
      get_record_payload_from_offsets(new_offset, remainder),
      copy_from->get_record_payload(copy_index),
      assorted::align8(payload_length));  // payload is zero-padded to 8 bytes, too.
  }

  // as we don't lock this page, we directly increment it to avoid is_locked assertion
  ++header_.key_count_;
}

bool MasstreeBorderPage::try_expand_record_in_page_physical(
  PayloadLength payload_count,
  SlotIndex record_index) {
  ASSERT_ND(!header_.snapshot_);
  ASSERT_ND(is_locked());
  ASSERT_ND(!is_moved());
  ASSERT_ND(record_index < get_key_count());
  DVLOG(2) << "Expanding record.. current max=" << get_max_payload_length(record_index)
    << ", which must become " << payload_count;

  ASSERT_ND(verify_slot_lengthes(record_index));
  Slot* old_slot = get_slot(record_index);
  ASSERT_ND(!old_slot->tid_.is_moved());
  ASSERT_ND(old_slot->tid_.is_keylocked());
  ASSERT_ND(!old_slot->does_point_to_layer());
  const SlotLengthPart lengthes = old_slot->read_lengthes_oneshot();
  const KeyLength remainder_length = old_slot->remainder_length_;
  const DataOffset record_length = to_record_length(remainder_length, payload_count);
  const DataOffset available = available_space();

  // 1. Trivial expansion if the record is placed at last. Fastest.
  if (get_next_offset() == lengthes.offset_ + lengthes.physical_record_length_) {
    const DataOffset diff = record_length - lengthes.physical_record_length_;
    DVLOG(1) << "Lucky, expanding a record at last record region. diff=" << diff;
    if (available >= diff) {
      DVLOG(2) << "woo. yes, we can just increase the length";
      old_slot->lengthes_.components.physical_record_length_ = record_length;
      assorted::memory_fence_release();
      increase_next_offset(diff);
      assorted::memory_fence_release();
      return true;
    }
  }

  // 2. In-page expansion. Fast.
  if (available >= record_length) {
    DVLOG(2) << "Okay, in-page record expansion.";
    // We have to make sure all threads see a valid state, either new or old.
    SlotLengthPart new_lengthes = lengthes;
    new_lengthes.offset_ = get_next_offset();
    new_lengthes.physical_record_length_ = record_length;
    const char* old_record = get_record_from_offset(lengthes.offset_);
    char* new_record = get_record_from_offset(new_lengthes.offset_);

    // 2-a. Create the new record region.
    if (lengthes.physical_record_length_ > 0) {
      std::memcpy(new_record, old_record, lengthes.physical_record_length_);
      assorted::memory_fence_release();
    }

    // 2-b. announce the new location in one-shot.
    old_slot->write_lengthes_oneshot(new_lengthes);
    assorted::memory_fence_release();
    // We don't have to change TID here because we did nothing logically.
    // Reading transactions are safe to read either old or new record regions.
    // See comments in MasstreeCommonLogType::apply_record_prepare() for how we make it safe
    // for writing transactions.

    increase_next_offset(record_length);
    assorted::memory_fence_release();
    return true;
  }

  // 3. ouch. we need to split the page to complete it. beyond this method.
  DVLOG(1) << "Umm, we need to split this page for record expansion. available="
    << available << ", record_length=" << record_length
    << ", record_index=" << record_index
    << ", key_count=" << get_key_count();
  return false;
}

void MasstreeBorderPage::initialize_as_layer_root_physical(
  VolatilePagePointer page_id,
  MasstreeBorderPage* parent,
  SlotIndex parent_index) {
  // This method assumes that the record's payload space is spacious enough.
  // The caller must make it sure as pre-condition.
  ASSERT_ND(parent->is_locked());
  ASSERT_ND(!parent->is_moved());
  Slot* parent_slot = parent->get_slot(parent_index);
  ASSERT_ND(parent_slot->tid_.is_keylocked());
  ASSERT_ND(!parent_slot->tid_.is_moved());
  ASSERT_ND(!parent_slot->does_point_to_layer());
  ASSERT_ND(parent->get_max_payload_length(parent_index) >= sizeof(DualPagePointer));
  DualPagePointer pointer;
  pointer.snapshot_pointer_ = 0;
  pointer.volatile_pointer_ = page_id;

  // initialize the root page by copying the record
  initialize_volatile_page(
    parent->header_.storage_id_,
    page_id,
    parent->get_layer() + 1,
    kInfimumSlice,    // infimum slice
    kSupremumSlice);   // high-fence is supremum
  ASSERT_ND(!is_locked());
  initialize_layer_root(parent, parent_index);
  ASSERT_ND(!is_moved());
  ASSERT_ND(!is_retired());

  SlotLengthPart new_lengthes = parent_slot->read_lengthes_oneshot();
  new_lengthes.payload_length_ = sizeof(DualPagePointer);
  char* parent_payload = parent->get_record_payload(parent_index);

  // point to the new page. Be careful on ordering.
  std::memcpy(parent_payload, &pointer, sizeof(pointer));
  assorted::memory_fence_release();
  parent_slot->write_lengthes_oneshot(new_lengthes);
  assorted::memory_fence_release();
  parent_slot->tid_.xct_id_.set_next_layer();  // which also turns off delete-bit

  ASSERT_ND(parent->get_next_layer(parent_index)->volatile_pointer_ == page_id);
  assorted::memory_fence_release();
}

void MasstreeIntermediatePage::verify_separators() const {
#ifndef NDEBUG
  if (is_empty_range()) {
    ASSERT_ND(get_key_count() == 0);
    ASSERT_ND(!is_moved());
    return;
  }
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

MasstreeBorderPage* MasstreeBorderPage::track_foster_child(
  KeySlice slice,
  const memory::GlobalVolatilePageResolver& resolver) {
  MasstreeBorderPage* cur_page = this;
  while (cur_page->is_moved()) {
    ASSERT_ND(cur_page->has_foster_child());
    ASSERT_ND(!cur_page->is_empty_range());
    if (cur_page->within_foster_minor(slice)) {
      ASSERT_ND(!cur_page->within_foster_major(slice));
      cur_page = reinterpret_cast<MasstreeBorderPage*>(
        resolver.resolve_offset(cur_page->get_foster_minor()));
    } else {
      ASSERT_ND(cur_page->within_foster_major(slice));
      cur_page = reinterpret_cast<MasstreeBorderPage*>(
        resolver.resolve_offset(cur_page->get_foster_major()));
    }
    ASSERT_ND(!cur_page->is_empty_range());
  }
  return cur_page;
}

xct::TrackMovedRecordResult MasstreeBorderPage::track_moved_record(
  Engine* engine,
  xct::RwLockableXctId* owner_address,
  xct::WriteXctAccess* /*write_set*/) {
  ASSERT_ND(owner_address->is_moved() || owner_address->is_next_layer());
  ASSERT_ND(!header().snapshot_);
  ASSERT_ND(header().get_page_type() == kMasstreeBorderPageType);

  // Slot begins with TID, so it's also the slot address
  Slot* slot = reinterpret_cast<Slot*>(owner_address);
  ASSERT_ND(&slot->tid_ == owner_address);
  const SlotIndex original_index = to_slot_index(slot);
  ASSERT_ND(original_index < kBorderPageMaxSlots);
  ASSERT_ND(original_index < get_key_count());

  const KeyLength remainder = slot->remainder_length_;
  // If it's originally a next-layer record, why we took it as a record? This shouldn't happen.
  ASSERT_ND(remainder != kInitiallyNextLayer);

  if (does_point_to_layer(original_index)) {
    return track_moved_record_next_layer(engine, owner_address);
  }

  // otherwise, we can track without key information within this layer.
  // the slice and key length is enough to identify the record.
  ASSERT_ND(is_moved());
  ASSERT_ND(has_foster_child());
  ASSERT_ND(!is_foster_minor_null());
  ASSERT_ND(!is_foster_major_null());
  const char* suffix = get_record(original_index);
  KeySlice slice = get_slice(original_index);

  // if remainder <= 8 : this layer can have only one record that has this slice and this length.
  // if remainder > 8  : this layer has either the exact record, or it's now a next layer pointer.

  // recursively track. although probably it's only one level
  MasstreeBorderPage* cur_page = this;
  const memory::GlobalVolatilePageResolver& resolver
    = engine->get_memory_manager()->get_global_volatile_page_resolver();
  while (true) {
    cur_page = cur_page->track_foster_child(slice, resolver);

    // now cur_page must be the page that contains the record.
    // the only exception is
    // 1) again the record is being moved concurrently
    // 2) the record was moved to another layer
    SlotIndex index = cur_page->find_key(slice, suffix, remainder);
    if (index == kBorderPageMaxSlots) {
      // this can happen rarely because we are not doing the stable version trick here.
      // this is rare, so we just abort. no safety violation.
      VLOG(0) << "Very interesting. moved record not found due to concurrent updates";
      return xct::TrackMovedRecordResult();
    }

    Slot* cur_slot = cur_page->get_slot(index);
    xct::RwLockableXctId* new_owner_address = &cur_slot->tid_;
    char* new_record_address = cur_page->get_record(index);
    if (cur_page->does_point_to_layer(index)) {
      // another rare case. the record has been now moved to another layer.
      if (remainder <= sizeof(KeySlice)) {
        // the record we are looking for can't be stored in next layer..
        VLOG(0) << "Wtf 2. moved record in next layer not found. Probably due to concurrent thread";
        return xct::TrackMovedRecordResult();
      }

      VLOG(0) << "Interesting. moved record are now in another layer. further track.";
      return cur_page->track_moved_record_next_layer(engine, new_owner_address);
    }

    // Otherwise, this is it!
    // be careful, we give get_record() as "payload". the word "payload" is a bit overused here.
    return xct::TrackMovedRecordResult(new_owner_address, new_record_address);
  }
}

xct::TrackMovedRecordResult MasstreeBorderPage::track_moved_record_next_layer(
  Engine* engine,
  xct::RwLockableXctId* owner_address) {
  ASSERT_ND(!header().snapshot_);
  ASSERT_ND(header().get_page_type() == kMasstreeBorderPageType);
  ASSERT_ND(owner_address->xct_id_.is_next_layer());

  Slot* slot = reinterpret_cast<Slot*>(owner_address);
  ASSERT_ND(&slot->tid_ == owner_address);
  const SlotIndex original_index = to_slot_index(slot);
  ASSERT_ND(original_index < kBorderPageMaxSlots);
  ASSERT_ND(original_index < get_key_count());
  ASSERT_ND(does_point_to_layer(original_index));

  // The new masstree page layout leaves the original suffix untouched.
  // Thus, we can always retrieve the original key from it no matter whether
  // the record moved offset to expand. In case the record was expanded in-page,
  // we use original_xxx below.
  const KeyLength remainder = slot->remainder_length_;
  ASSERT_ND(remainder != kInitiallyNextLayer);
  if (UNLIKELY(remainder <= sizeof(KeySlice))) {
    LOG(ERROR) << "WTF. The original record was too short to get moved to next-layer, but it did?";
    return xct::TrackMovedRecordResult();
  }

  const char* original_suffix = get_record(original_index);
  const uint8_t cur_layer = get_layer();
  const uint8_t next_layer = cur_layer + 1U;
  const KeySlice next_slice = slice_key(original_suffix, remainder - sizeof(KeySlice));
  const KeyLength next_remainder = remainder - sizeof(KeySlice);
  const char* next_suffix = original_suffix + sizeof(KeySlice);

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
    // 2) the record was moved to another layer
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(cur_page);
    // we track foster child in border pages only
    casted = casted->track_foster_child(next_slice, resolver);
    ASSERT_ND(casted != this);
    SlotIndex index = casted->find_key(next_slice, next_suffix, next_remainder);
    if (index == kBorderPageMaxSlots) {
      VLOG(0) << "Very interesting. moved record not found due to concurrent updates";
      return xct::TrackMovedRecordResult();
    }

    xct::RwLockableXctId* new_owner_address = casted->get_owner_id(index);
    ASSERT_ND(new_owner_address != owner_address);
    char* new_record_address = casted->get_record(index);
    if (casted->does_point_to_layer(index)) {
      // the record has been now moved to yet another layer.
      if (next_remainder <= sizeof(KeySlice)) {
        // the record we are looking for can't be stored in next layer..
        VLOG(0) << "Wtf 2. moved record in next layer not found. Probably due to concurrent thread";
        return xct::TrackMovedRecordResult();
      }

      VLOG(0) << "Interesting. moved record are now in another layer. further track.";
      return casted->track_moved_record_next_layer(engine, new_owner_address);
    }

    // be careful, we give get_record() as "payload". the word "payload" is a bit overused here.
    return xct::TrackMovedRecordResult(new_owner_address, new_record_address);
  }
}

bool MasstreeBorderPage::verify_slot_lengthes(SlotIndex index) const {
  const Slot* slot = get_slot(index);
  SlotLengthPart lengthes = slot->lengthes_.components;
  if (lengthes.offset_ % 8 != 0) {
    ASSERT_ND(false);
    return false;
  }
  if (lengthes.physical_record_length_ % 8 != 0) {
    ASSERT_ND(false);
    return false;
  }
  if (lengthes.offset_ + lengthes.physical_record_length_ > sizeof(data_)) {
    ASSERT_ND(false);
    return false;
  }

  if (slot->remainder_length_ == kInitiallyNextLayer) {
    if (!slot->does_point_to_layer()) {
      ASSERT_ND(false);
      return false;
    }
  }
  // the only case we move the record in-page is to get a larger record size.
  if (lengthes.offset_ != slot->original_offset_) {
    if (lengthes.offset_ < slot->original_offset_) {
      ASSERT_ND(false);
      return false;
    }
    if (lengthes.physical_record_length_ <= slot->original_physical_record_length_) {
      ASSERT_ND(false);
      return false;
    }
  }

  if (slot->does_point_to_layer()) {
    if (slot->remainder_length_ <= sizeof(KeySlice)) {
      ASSERT_ND(false);
      return false;
    }
    if (lengthes.payload_length_ != sizeof(DualPagePointer)) {
      ASSERT_ND(false);
      return false;
    }
  } else {
    if (lengthes.payload_length_ > slot->get_max_payload_peek()) {
      ASSERT_ND(false);
      return false;
    }
  }
  return true;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
