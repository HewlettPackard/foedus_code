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
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

#include <glog/logging.h>

#include <cstring>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/hash/hash_combo.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {

ErrorStack HashStoragePimpl::drop() {
  LOG(INFO) << "Uninitializing an hash-storage " << get_name();

  if (control_block_->root_page_pointer_.volatile_pointer_.components.offset) {
    // release volatile pages
    const memory::GlobalVolatilePageResolver& page_resolver
      = engine_->get_memory_manager()->get_global_volatile_page_resolver();
    HashIntermediatePage* root = reinterpret_cast<HashIntermediatePage*>(
      page_resolver.resolve_offset(control_block_->root_page_pointer_.volatile_pointer_));
    root->release_pages_recursive_parallel(engine_);
    control_block_->root_page_pointer_.volatile_pointer_.word = 0;
  }

  return kRetOk;
}


ErrorStack HashStoragePimpl::create(const HashMetadata& metadata) {
  if (exists()) {
    LOG(ERROR) << "This hash-storage already exists: " << get_name();
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  control_block_->meta_ = metadata;
  LOG(INFO) << "Newly creating an hash-storage " << get_name();
  control_block_->bin_count_ = 1ULL << get_bin_bits();
  control_block_->levels_ = bins_to_level(control_block_->bin_count_);
  ASSERT_ND(control_block_->levels_ >= 1U);
  ASSERT_ND(control_block_->bin_count_ <= fanout_power(control_block_->levels_));
  ASSERT_ND(control_block_->bin_count_ > fanout_power(control_block_->levels_ - 1U));
  LOG(INFO) << "bin_count=" << get_bin_count() << ", levels=" << static_cast<int>(get_levels());

  // small number of root pages. we should at least have that many free pages.
  // so far grab all of them from node 0. no round robbin
  memory::PagePool* pool = engine_->get_memory_manager()->get_node_memory(0)->get_volatile_pool();
  const memory::LocalPageResolver &local_resolver = pool->get_resolver();

  // allocate only the root page
  memory::PagePoolOffset root_offset;
  WRAP_ERROR_CODE(pool->grab_one(&root_offset));
  ASSERT_ND(root_offset);
  HashIntermediatePage* root_page = reinterpret_cast<HashIntermediatePage*>(
    local_resolver.resolve_offset_newpage(root_offset));
  control_block_->root_page_pointer_.snapshot_pointer_ = 0;
  control_block_->root_page_pointer_.volatile_pointer_ = combine_volatile_page_pointer(
    0,
    0,
    0,
    root_offset);
  HashBinRange root_bin_range(0, control_block_->bin_count_);
  root_page->initialize_volatile_page(
    get_id(),
    control_block_->root_page_pointer_.volatile_pointer_,
    nullptr,
    root_bin_range);
  root_page->header().set_in_layer_level(control_block_->levels_ - 1U);

  LOG(INFO) << "Newly created an hash-storage " << get_name();
  control_block_->status_ = kExists;
  return kRetOk;
}

ErrorStack HashStoragePimpl::load(const StorageControlBlock& snapshot_block) {
  control_block_->meta_ = static_cast<const HashMetadata&>(snapshot_block.meta_);
  const HashMetadata& meta = control_block_->meta_;
  control_block_->bin_count_ = 1ULL << get_bin_bits();
  control_block_->levels_ = bins_to_level(control_block_->bin_count_);
  control_block_->root_page_pointer_.snapshot_pointer_ = meta.root_snapshot_page_id_;
  control_block_->root_page_pointer_.volatile_pointer_.word = 0;

  // Root page always has volatile version.
  // Construct it from snapshot version.
  cache::SnapshotFileSet fileset(engine_);
  CHECK_ERROR(fileset.initialize());
  UninitializeGuard fileset_guard(&fileset, UninitializeGuard::kWarnIfUninitializeError);

  // load root page
  VolatilePagePointer volatile_pointer;
  HashIntermediatePage* volatile_root;
  CHECK_ERROR(engine_->get_memory_manager()->load_one_volatile_page(
    &fileset,
    meta.root_snapshot_page_id_,
    &volatile_pointer,
    reinterpret_cast<Page**>(&volatile_root)));
  control_block_->root_page_pointer_.volatile_pointer_ = volatile_pointer;

  CHECK_ERROR(fileset.uninitialize());

  LOG(INFO) << "Loaded a hash-storage " << get_name();
  control_block_->status_ = kExists;
  return kRetOk;
}

HashIntermediatePage* HashStoragePimpl::get_root_page() {
  // TODO(Hideaki) SI should return snapshot version
  return reinterpret_cast<HashIntermediatePage*>(
    engine_->get_memory_manager()->get_global_volatile_page_resolver().resolve_offset(
      control_block_->root_page_pointer_.volatile_pointer_));
}

ErrorCode HashStoragePimpl::get_record(
  thread::Thread* context,
  const HashCombo& combo,
  void* payload,
  uint16_t* payload_capacity) {
  CHECK_ERROR_CODE(lookup_bin(context, false, combo));
  DVLOG(3) << "get_hash: hash combo=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, combo));

  if (combo.record_) {
    // we already added to read set, so the only remaining thing is to read payload
    if (combo.payload_length_ > *payload_capacity) {
      // buffer too small
      DVLOG(0) << "buffer too small??" << combo;
      *payload_capacity = combo.payload_length_;
      return kErrorCodeStrTooSmallPayloadBuffer;
    }
    *payload_capacity = combo.payload_length_;
    std::memcpy(payload, combo.record_->payload_ + key_length, combo.payload_length_);
    return kErrorCodeOk;
  } else {
    // not found
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }
}

template <typename PAYLOAD>
ErrorCode HashStoragePimpl::get_record_primitive(
  thread::Thread* context,
  const HashCombo& combo,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  CHECK_ERROR_CODE(lookup_bin(context, false, &combo));
  DVLOG(3) << "get_hash_primitive: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_) {
    // we already added to read set, so the only remaining thing is to read payload
    if (combo.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
      // payload too small
      LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
      return kErrorCodeStrTooShortPayload;
    }
    std::memcpy(payload, combo.record_->payload_ + key_length + payload_offset, sizeof(PAYLOAD));
    return kErrorCodeOk;
  } else {
    // not found
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }
}

ErrorCode HashStoragePimpl::get_record_part(
  thread::Thread* context,
  const HashCombo& combo,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  CHECK_ERROR_CODE(lookup_bin(context, false, &combo));
  DVLOG(3) << "get_hash_part: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_) {
    if (combo.payload_length_ < payload_offset + payload_count) {
      LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
      return kErrorCodeStrTooShortPayload;
    }
    std::memcpy(payload, combo.record_->payload_ + key_length + payload_offset, payload_count);
    return kErrorCodeOk;
  } else {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }
}



ErrorCode HashStoragePimpl::insert_record(
  thread::Thread* context,
  const HashCombo& combo,
  const void* payload,
  uint16_t payload_count) {
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "insert_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_) {
    return kErrorCodeStrKeyAlreadyExists;
  }

  // lookup_bin should have already created volatile pages for both cases
  HashBinPage* bin_page = combo.bin_pages_[choice];
  ASSERT_ND(bin_page && !bin_page->header().snapshot_);
  ASSERT_ND(bin_page->header().get_page_type() == kHashBinPageType);
  HashDataPage* data_page = combo.data_pages_[choice];
  ASSERT_ND(data_page && !data_page->header().snapshot_);
  ASSERT_ND(data_page->header().get_page_type() == kHashDataPageType);

  uint16_t log_length = HashInsertLogType::calculate_log_length(key_length, payload_count);
  HashInsertLogType* log_entry = reinterpret_cast<HashInsertLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), key, key_length, bin1, combo.tag_, payload, payload_count);
  // NOTE tentative hack! currently "payload address" for hash insert write set points to bin page
  // so that we don't have to calculate it again.
  return context->get_current_xct().add_to_write_set(
    get_id(),
    &(data_page->page_owner()),
    reinterpret_cast<char*>(bin_page),
    log_entry);
}

ErrorCode HashStoragePimpl::delete_record(
  thread::Thread* context,
  const HashCombo& combo) {
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "delete_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  uint16_t log_length = HashDeleteLogType::calculate_log_length(key_length);
  HashDeleteLogType* log_entry = reinterpret_cast<HashDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), key, key_length, combo.record_bin1_, combo.record_slot_);
  // NOTE tentative hack! currently "payload address" for hash insert write set points to bin page
  // so that we don't have to calculate it again.
  return context->get_current_xct().add_to_write_set(
    get_id(),
    &combo.record_->owner_id_,
    reinterpret_cast<char*>(bin_page),
    log_entry);
}

ErrorCode HashStoragePimpl::overwrite_record(
  thread::Thread* context ,
  const HashCombo& combo,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "overwrite_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  if (combo.payload_length_ < payload_offset + payload_count) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

#ifndef NDEBUG
  HashBinPage* bin_page = combo.bin_pages_[combo.record_bin1_ ? 0 : 1];
  ASSERT_ND(bin_page && !bin_page->header().snapshot_);
  HashDataPage* data_page = combo.data_pages_[combo.record_bin1_ ? 0 : 1];
  ASSERT_ND(data_page && !data_page->header().snapshot_);
#endif  // NDEBUG

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, payload_count);
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    key,
    key_length,
    combo.record_bin1_,
    combo.record_slot_,
    payload,
    payload_offset,
    payload_count);
  return context->get_current_xct().add_to_write_set(get_id(), combo.record_, log_entry);
}

template <typename PAYLOAD>
ErrorCode HashStoragePimpl::overwrite_record_primitive(
  thread::Thread* context,
  const HashCombo& combo,
  PAYLOAD payload,
  uint16_t payload_offset) {
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "overwrite_hash_primitive: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  if (combo.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, sizeof(PAYLOAD));
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    key,
    key_length,
    combo.record_bin1_,
    combo.record_slot_,
    &payload,
    payload_offset,
    sizeof(PAYLOAD));
  return context->get_current_xct().add_to_write_set(get_id(), combo.record_, log_entry);
}

template <typename PAYLOAD>
ErrorCode HashStoragePimpl::increment_record(
  thread::Thread* context,
  const HashCombo& combo,
  PAYLOAD* value,
  uint16_t payload_offset) {
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "increment_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  if (combo.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  char* ptr = combo.record_->payload_ + key_length + payload_offset;
  PAYLOAD old_value = *reinterpret_cast<const PAYLOAD*>(ptr);
  *value += old_value;

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, sizeof(PAYLOAD));
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    key,
    key_length,
    combo.record_bin1_,
    combo.record_slot_,
    value,
    payload_offset,
    sizeof(PAYLOAD));
  return context->get_current_xct().add_to_write_set(get_id(), combo.record_, log_entry);
}

inline HashDataPage* to_page(xct::LockableXctId* owner_id) {
  // super-dirty way to obtain Page the record belongs to.
  // because all pages are 4kb aligned, we can just divide and multiply.
  uintptr_t int_address = reinterpret_cast<uintptr_t>(reinterpret_cast<void*>(owner_id));
  uint64_t aligned_address = static_cast<uint64_t>(int_address) / kPageSize * kPageSize;
  return reinterpret_cast<HashDataPage*>(
    reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(aligned_address)));
}

inline HashRootPage* HashStoragePimpl::lookup_boundary_root(
  thread::Thread* context,
  uint64_t bin,
  uint16_t* pointer_index) {
  uint64_t bin_page = bin / kBinsPerPage;
  if (get_root_pages() == 1) {
    ASSERT_ND(bin_page <= kHashRootPageFanout);
    *pointer_index = bin_page;
    return get_root_page();
  } else {
    uint64_t child_root = bin_page / kHashRootPageFanout;
    VolatilePagePointer pointer = get_root_page()->pointer(child_root).volatile_pointer_;
    ASSERT_ND(pointer.components.offset != 0);
    *pointer_index = bin_page % kHashRootPageFanout;
    return reinterpret_cast<HashRootPage*>(
      context->get_global_volatile_page_resolver().resolve_offset(pointer));
  }
}

ErrorCode HashStoragePimpl::lookup_bin(thread::Thread* context, bool for_write, HashCombo* combo) {
  // TODO(Hideaki) to speed up the following, we should prefetch more aggressively.
  // the code will be a bit uglier, though.

  // For writes, we take care of node-set later separately.
  // TODO(Hideaki) let's blindly load volatile pages for all writes.
  // it simplifies this method a lot.

  // find bin page
  for (uint8_t i = 0; i < 2; ++i) {
    uint16_t pointer_index;
    HashRootPage* boundary = lookup_boundary_root(context, combo->bins_[i], &pointer_index);
    CHECK_ERROR_CODE(context->follow_page_pointer(
      hash_bin_volatile_page_init,
      !for_write,  // tolerate null page for read. if that happens, we get nullptr on the bin page
      for_write,  // always get volatile pages for writes
      true,
      &(boundary->pointer(pointer_index)),
      reinterpret_cast<Page**>(&(combo->bin_pages_[i])),
      reinterpret_cast<const Page*>(boundary),
      pointer_index));
  }

  // read bin pages
  for (uint8_t i = 0; i < 2; ++i) {
    uint16_t bin_pos = combo->bins_[i] % kBinsPerPage;
    HashBinPage* bin_page = combo->bin_pages_[i];
    if (bin_page) {
      bool snapshot = bin_page->header().snapshot_;
      if (i == 0 && combo->bin_pages_[1]) {
        // when we are reading from both of them we prefetch the two 64 bytes.
        // if we are reading from only one of them, no need.
        assorted::prefetch_cacheline(&(combo->bin_pages_[0]->bin(bin_pos)));
        uint16_t another_pos = combo->bins_[1] % kBinsPerPage;
        assorted::prefetch_cacheline(&(combo->bin_pages_[1]->bin(another_pos)));
      }

      HashBinPage::Bin& bin = combo->bin_pages_[i]->bin(bin_pos);
      // add the mod counter to read set. we must do it BEFORE reading tags
      // and then take fence (consume is enough).
      if (!snapshot) {  // if we are reading a snapshot page, doesn't matter
        combo->observed_mod_count_[i] = bin.mod_counter_;
        assorted::memory_fence_consume();
      }
      combo->hit_bitmap_[i] = iterate_over_tags(bin, combo->tag_);

      // obtain data pages, but not read it yet. it's done in locate_record()
      if (combo->hit_bitmap_[i] || for_write) {
        // becauase it's hitting. either page must be non-null
        CHECK_ERROR_CODE(context->follow_page_pointer(
          hash_data_volatile_page_init,
          !for_write,  // tolerate null page for read
          for_write,  // always get volatile pages for writes
          !snapshot && !for_write,  // if bin page is snapshot, data page is stable
          &(bin.data_pointer_),
          reinterpret_cast<Page**>(&(combo->data_pages_[i])),
          reinterpret_cast<const Page*>(bin_page),
          bin_pos));
      } else {
        combo->data_pages_[i] = nullptr;
      }
    }
  }
  return kErrorCodeOk;
}

inline ErrorCode HashStoragePimpl::locate_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  HashCombo* combo) {
  // TODO(Hideaki) prefetch.
  // TODO(Hideaki) some of the read set below is not mandatory depending on operation type.
  xct::Xct& current_xct = context->get_current_xct();
  combo->record_ = nullptr;
  for (uint8_t i = 0; i < 2; ++i) {
    if (combo->bin_pages_[i] == nullptr || combo->data_pages_[i] == nullptr) {
      continue;
    }
    HashDataPage* data_page = combo->data_pages_[i];

    // add page lock for read set. before accessing records
    current_xct.add_to_read_set(
      get_id(),
      data_page->page_owner().xct_id_,
      &data_page->page_owner());

    uint32_t hit_bitmap = combo->hit_bitmap_[i];
    for (uint8_t rec = 0; rec < data_page->get_record_count(); ++rec) {
      if ((hit_bitmap & (1U << rec)) == 0) {
        continue;
      }
      // okay, this record has the same tag. let's check this by comparing the whole key.
      const HashDataPage::Slot& slot = data_page->slot(rec);
      if (slot.flags_ & HashDataPage::kFlagDeleted) {
        continue;
      } else if (slot.key_length_ != key_length) {
        continue;
      }
      // TODO(Hideaki) handle kFlagStoredInNextPages.
      Record* record = data_page->interpret_record(slot.offset_);
      current_xct.add_to_read_set(
        get_id(),
        record->owner_id_.xct_id_,
        &record->owner_id_);
      if (std::memcmp(record->payload_, key, key_length) == 0) {
        // okay, matched!!
        // once we exactly locate the record, no point to check other records/bin. exit
        combo->record_ = record;
        combo->record_bin1_ = (i == 0);
        combo->record_slot_ = rec;
        combo->payload_length_ = slot.record_length_ - key_length - kRecordOverhead;
        return kErrorCodeOk;
      }
    }
  }
  return kErrorCodeOk;
}


// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_2I(x) template ErrorCode HashStoragePimpl::get_record_primitive< x > \
  (thread::Thread* context, const HashCombo& combo, x* payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_2I);

#define EXPIN_3I(x) template ErrorCode HashStoragePimpl::overwrite_record_primitive< x > \
  (thread::Thread* context, const HashCombo& combo, x payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_3I);

#define EXPIN_5I(x) template ErrorCode HashStoragePimpl::increment_record< x > \
  (thread::Thread* context, const HashCombo& combo, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5I);
// @endcond

}  // namespace hash
}  // namespace storage
}  // namespace foedus
