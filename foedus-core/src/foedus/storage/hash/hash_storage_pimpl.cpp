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
#include "foedus/storage/hash/hash_record_location.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {

ErrorStack HashStoragePimpl::drop() {
  LOG(INFO) << "Uninitializing an hash-storage " << get_name();

  if (!control_block_->root_page_pointer_.volatile_pointer_.is_null()) {
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

  // hash-specific check.
  // Due to the current design of hash_partitioner, we spend hashbins bytes
  // out of the partitioner memory.
  uint64_t required_partitioner_bytes = metadata.get_bin_count() + 4096ULL;
  uint64_t partitioner_bytes
    = engine_->get_options().storage_.partitioner_data_memory_mb_ * (1ULL << 20);
  // we don't bother checking other storages' consumption. the config might later change anyways.
  // Instead, leave a bit of margin (25%) for others.
  if (partitioner_bytes < required_partitioner_bytes * 1.25) {
    std::stringstream str;
    str << metadata << ".\n"
      << "To accomodate this number of hash bins, partitioner_data_memory_mb_ must be"
      << " at least " << (required_partitioner_bytes * 1.25 / (1ULL << 20));
    return ERROR_STACK_MSG(kErrorCodeStrHashBinsTooMany, str.str().c_str());
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
  const uint16_t kTheNode = 0;
  memory::PagePool* pool
    = engine_->get_memory_manager()->get_node_memory(kTheNode)->get_volatile_pool();
  const memory::LocalPageResolver &local_resolver = pool->get_resolver();

  // allocate only the root page
  memory::PagePoolOffset root_offset;
  WRAP_ERROR_CODE(pool->grab_one(&root_offset));
  ASSERT_ND(root_offset);
  HashIntermediatePage* root_page = reinterpret_cast<HashIntermediatePage*>(
    local_resolver.resolve_offset_newpage(root_offset));
  control_block_->root_page_pointer_.snapshot_pointer_ = 0;
  control_block_->root_page_pointer_.volatile_pointer_.set(kTheNode, root_offset);
  root_page->initialize_volatile_page(
    get_id(),
    control_block_->root_page_pointer_.volatile_pointer_,
    nullptr,
    control_block_->levels_ - 1U,
    0);
  root_page->assert_range();

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

ErrorCode HashStoragePimpl::get_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  void* payload,
  uint16_t* payload_capacity,
  bool read_only) {
  HashDataPage* bin_head;
  CHECK_ERROR_CODE(locate_bin(context, !read_only, combo, &bin_head));
  if (!bin_head) {
    return kErrorCodeStrKeyNotFound;  // protected by pointer set, so we are done
  }
  RecordLocation location;
  CHECK_ERROR_CODE(locate_record_logical(
    context,
    !read_only,
    false,
    0,
    key,
    key_length,
    combo,
    bin_head,
    &location));
  if (!location.is_found()) {
    return kErrorCodeStrKeyNotFound;  // protected by page version set, so we are done
  }

  // here, we do NOT have to do another optimistic-read protocol because we already took
  // the owner_id into read-set. If this read is corrupted, we will be aware of it at commit time.
  uint16_t payload_length = location.cur_payload_length_;
  if (payload_length > *payload_capacity) {
    // buffer too small
    DVLOG(0) << "buffer too small??" << payload_length << ":" << *payload_capacity;
    *payload_capacity = payload_length;
    return kErrorCodeStrTooSmallPayloadBuffer;
  }

  *payload_capacity = payload_length;
  uint16_t key_offset = location.get_aligned_key_length();
  std::memcpy(payload, location.record_ + key_offset, payload_length);
  return kErrorCodeOk;
}

ErrorCode HashStoragePimpl::get_record_part(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count,
  bool read_only) {
  HashDataPage* bin_head;
  CHECK_ERROR_CODE(locate_bin(context, !read_only, combo, &bin_head));
  if (!bin_head) {
    return kErrorCodeStrKeyNotFound;  // protected by pointer set, so we are done
  }
  RecordLocation location;
  CHECK_ERROR_CODE(locate_record_logical(
    context,
    !read_only,
    false,
    0,
    key,
    key_length,
    combo,
    bin_head,
    &location));
  if (!location.is_found()) {
    return kErrorCodeStrKeyNotFound;  // protected by page version set, so we are done
  }

  uint16_t payload_length = location.cur_payload_length_;
  if (payload_length < payload_offset + payload_count) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  uint16_t key_offset = location.get_aligned_key_length();
  std::memcpy(payload, location.record_ + key_offset + payload_offset, payload_count);
  return kErrorCodeOk;
}

uint16_t adjust_payload_hint(uint16_t payload_count, uint16_t physical_payload_hint) {
  ASSERT_ND(physical_payload_hint >= payload_count);  // if not, most likely misuse.
  if (physical_payload_hint < payload_count) {
    physical_payload_hint = payload_count;
  }
  physical_payload_hint = assorted::align8(physical_payload_hint);
  return physical_payload_hint;
}


ErrorCode HashStoragePimpl::register_record_write_log(
  thread::Thread* context,
  const RecordLocation& location,
  log::RecordLogType* log_entry) {
  // If we have taken readset in locate_record, add as a related write set
  ASSERT_ND(location.is_found());
  auto* slot = location.page_->get_slot_address(location.index_);
  char* record = location.record_;
  xct::Xct* cur_xct = &context->get_current_xct();
  if (location.readset_) {
    return cur_xct->add_related_write_set(location.readset_, &slot->tid_, record, log_entry);
  } else {
    return cur_xct->add_to_write_set(get_id(), &slot->tid_, record, log_entry);
  }
}


ErrorCode HashStoragePimpl::insert_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  const void* payload,
  uint16_t payload_count,
  uint16_t physical_payload_hint) {
  physical_payload_hint = adjust_payload_hint(payload_count, physical_payload_hint);

  HashDataPage* bin_head;
  CHECK_ERROR_CODE(locate_bin(context, true, combo, &bin_head));
  ASSERT_ND(bin_head);

  while (true) {  // we might retry due to migrate_record. not that often, tho.
    RecordLocation location;
    CHECK_ERROR_CODE(locate_record_logical(
      context,
      true,
      true,
      physical_payload_hint,
      key,
      key_length,
      combo,
      bin_head,
      &location));

    // we create if not exists, these are surely non-null
    ASSERT_ND(location.is_found());
    ASSERT_ND(location.record_);

    // but, that record might be not logically deleted
    if (!location.observed_.is_deleted()) {
      return kErrorCodeStrKeyAlreadyExists;  // protected by the read set
    }

    if (payload_count > location.get_max_payload()) {
      // The physical record is too short. It will trigger a record expansion, which is a
      // system transaction (logically does nothing!) to migrate this deleted record.
      HashDataPage* cur_page = location.page_;
      ASSERT_ND(!cur_page->header().snapshot_);
      ASSERT_ND(cur_page->bloom_filter().contains(combo.fingerprint_));
      DataPageSlotIndex cur_index = location.index_;
      DVLOG(2) << "Record expansion triggered. payload_count=" << payload_count
        << ", current max=" << location.get_max_payload()
        << ", size hint=" << physical_payload_hint;

      CHECK_ERROR_CODE(migrate_record_physical(
        context,
        key,
        key_length,
        combo,
        cur_page,
        cur_index,
        physical_payload_hint));
      DVLOG(2) << "Expanded record!";
      // need to re-locate. also, beacuse the above method is physical-only,
      // the moved location might be again moved or now deleted. easise to just retry.
      continue;
    }

    uint16_t log_length = HashInsertLogType::calculate_log_length(key_length, payload_count);
    HashInsertLogType* log_entry = reinterpret_cast<HashInsertLogType*>(
      context->get_thread_log_buffer().reserve_new_log(log_length));
    log_entry->populate(
      get_id(),
      key,
      key_length,
      get_bin_bits(),
      combo.hash_,
      payload,
      payload_count);

    return register_record_write_log(context, location, log_entry);
  }
}

ErrorCode HashStoragePimpl::delete_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo) {
  HashDataPage* bin_head;
  CHECK_ERROR_CODE(locate_bin(context, true, combo, &bin_head));
  ASSERT_ND(bin_head);
  RecordLocation location;
  CHECK_ERROR_CODE(locate_record_logical(
    context,
    true,
    false,
    0,
    key,
    key_length,
    combo,
    bin_head,
    &location));

  if (!location.is_found()) {
    return kErrorCodeStrKeyNotFound;  // protected by page version set, so we are done
  } else if (location.observed_.is_deleted()) {
    return kErrorCodeStrKeyNotFound;  // protected by the read set
  }

  uint16_t log_length = HashDeleteLogType::calculate_log_length(key_length, 0);
  HashDeleteLogType* log_entry = reinterpret_cast<HashDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), key, key_length, get_bin_bits(), combo.hash_);

  return register_record_write_log(context, location, log_entry);
}

ErrorCode HashStoragePimpl::upsert_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  const void* payload,
  uint16_t payload_count,
  uint16_t physical_payload_hint) {
  physical_payload_hint = adjust_payload_hint(payload_count, physical_payload_hint);

  // Upsert is a combination of what insert does and what delete does.
  // If there isn't an existing physical record, it's exactly same as insert.
  // If there is, it's _basically_ a delete followed by an insert.
  // There are a few complications, depending on the status of the record.

  HashDataPage* bin_head;
  CHECK_ERROR_CODE(locate_bin(context, true, combo, &bin_head));
  ASSERT_ND(bin_head);

  while (true) {  // we might retry due to migrate_record. not that often, tho.
    RecordLocation location;
    CHECK_ERROR_CODE(locate_record_logical(
      context,
      true,
      true,
      physical_payload_hint,
      key,
      key_length,
      combo,
      bin_head,
      &location));

    // we create if not exists, these are surely non-null
    ASSERT_ND(location.is_found());
    ASSERT_ND(location.record_);

    // Whether currently deleted or not, migrate it to make sure the record is long enough.
    if (payload_count > location.get_max_payload()) {
      HashDataPage* cur_page = location.page_;
      ASSERT_ND(!cur_page->header().snapshot_);
      ASSERT_ND(cur_page->bloom_filter().contains(combo.fingerprint_));
      DataPageSlotIndex cur_index = location.index_;
      DVLOG(2) << "Record expansion triggered. payload_count=" << payload_count
        << ", current max=" << location.get_max_payload()
        << ", size hint=" << physical_payload_hint;

      CHECK_ERROR_CODE(migrate_record_physical(
        context,
        key,
        key_length,
        combo,
        cur_page,
        cur_index,
        physical_payload_hint));
      DVLOG(2) << "Expanded record!";
      continue;  // need to re-locate the record. retry.
    }

    ASSERT_ND(payload_count <= location.get_max_payload());
    HashCommonLogType* log_common;
    if (location.observed_.is_deleted()) {
      // If it's a deleted record, this turns to be a plain insert.
      uint16_t log_length = HashInsertLogType::calculate_log_length(key_length, payload_count);
      HashInsertLogType* log_entry = reinterpret_cast<HashInsertLogType*>(
        context->get_thread_log_buffer().reserve_new_log(log_length));
      log_entry->populate(
        get_id(),
        key,
        key_length,
        get_bin_bits(),
        combo.hash_,
        payload,
        payload_count);
      log_common = log_entry;
    } else if (location.cur_payload_length_ == payload_count) {
      // If it's not changing payload size of existing record, we can conver it to an overwrite,
      // which is more efficient
      uint16_t log_length = HashUpdateLogType::calculate_log_length(key_length, payload_count);
      HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
        context->get_thread_log_buffer().reserve_new_log(log_length));
      log_entry->populate(
        get_id(),
        key,
        key_length,
        get_bin_bits(),
        combo.hash_,
        payload,
        0,
        payload_count);
      log_common = log_entry;
    } else {
      // If not, this is an update operation.
      uint16_t log_length = HashUpdateLogType::calculate_log_length(key_length, payload_count);
      HashUpdateLogType* log_entry = reinterpret_cast<HashUpdateLogType*>(
        context->get_thread_log_buffer().reserve_new_log(log_length));
      log_entry->populate(
        get_id(),
        key,
        key_length,
        get_bin_bits(),
        combo.hash_,
        payload,
        payload_count);
      log_common = log_entry;
    }

    return register_record_write_log(context, location, log_common);
  }
}

ErrorCode HashStoragePimpl::overwrite_record(
  thread::Thread* context ,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  HashDataPage* bin_head;
  CHECK_ERROR_CODE(locate_bin(context, true, combo, &bin_head));
  ASSERT_ND(bin_head);
  RecordLocation location;
  CHECK_ERROR_CODE(locate_record_logical(
    context,
    true,
    false,
    0,
    key,
    key_length,
    combo,
    bin_head,
    &location));

  if (!location.is_found()) {
    return kErrorCodeStrKeyNotFound;  // protected by page version set, so we are done
  } else if (location.observed_.is_deleted()) {
    return kErrorCodeStrKeyNotFound;  // protected by the read set
  } else if (location.cur_payload_length_ < payload_offset + payload_count) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;  // protected by the read set
  }

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, payload_count);
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    key,
    key_length,
    get_bin_bits(),
    combo.hash_,
    payload,
    payload_offset,
    payload_count);

  // overwrite_record is apparently a blind-write, but actually it's not.
  // we depend on the fact that the record was not deleted/moved! so,
  // this still has a related/dependent read-set
  return register_record_write_log(context, location, log_entry);
}

template <typename PAYLOAD>
ErrorCode HashStoragePimpl::increment_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  PAYLOAD* value,
  uint16_t payload_offset) {
  HashDataPage* bin_head;
  CHECK_ERROR_CODE(locate_bin(context, true, combo, &bin_head));
  ASSERT_ND(bin_head);
  RecordLocation location;
  CHECK_ERROR_CODE(locate_record_logical(
    context,
    true,
    false,
    0,
    key,
    key_length,
    combo,
    bin_head,
    &location));

  if (!location.is_found()) {
    return kErrorCodeStrKeyNotFound;  // protected by page version set, so we are done
  } else if (location.observed_.is_deleted()) {
    return kErrorCodeStrKeyNotFound;  // protected by the read set
  } else if (location.cur_payload_length_ < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;  // protected by the read set
  }

  // value: (in) addendum, (out) value after addition.
  PAYLOAD* current = reinterpret_cast<PAYLOAD*>(
    location.record_ + location.get_aligned_key_length() + payload_offset);
  *value += *current;

  uint16_t log_length
    = HashOverwriteLogType::calculate_log_length(key_length, sizeof(PAYLOAD));
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    get_id(),
    key,
    key_length,
    get_bin_bits(),
    combo.hash_,
    value,
    payload_offset,
    sizeof(PAYLOAD));

  return register_record_write_log(context, location, log_entry);
}

ErrorCode HashStoragePimpl::get_root_page(
  thread::Thread* context,
  bool for_write,
  HashIntermediatePage** root) {
  CHECK_ERROR_CODE(context->follow_page_pointer(
    nullptr,  // guaranteed to be non-null
    false,    // guaranteed to be non-null
    for_write,
    false,    // guaranteed to be non-null
    &control_block_->root_page_pointer_,
    reinterpret_cast<Page**>(root),
    nullptr,  // no parent. it's root.
    0));
  ASSERT_ND((*root)->header().get_page_type() == kHashIntermediatePageType);
  ASSERT_ND((*root)->get_level() + 1U == control_block_->levels_);
  return kErrorCodeOk;
}

ErrorCode HashStoragePimpl::follow_page(
  thread::Thread* context,
  bool for_write,
  HashIntermediatePage* parent,
  uint16_t index_in_parent,
  Page** page) {
  ASSERT_ND(index_in_parent < kHashIntermediatePageFanout);
  ASSERT_ND(parent);
  ASSERT_ND(parent->header().get_page_type() == kHashIntermediatePageType);
  bool is_parent_snapshot = parent->header().snapshot_;
  uint8_t parent_level = parent->get_level();
  ASSERT_ND(!is_parent_snapshot || !for_write);

  DualPagePointer& pointer = parent->get_pointer(index_in_parent);
  bool child_intermediate = (parent_level > 0);
  if (is_parent_snapshot) {
    // if we are in snapshot world, there is no choice.
    // separating this out also handles SI level well.
    ASSERT_ND(!for_write);
    if (pointer.snapshot_pointer_ == 0) {
      *page = nullptr;
    } else {
      CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(pointer.snapshot_pointer_, page));
      ASSERT_ND((*page)->get_header().snapshot_);
    }
  } else if (child_intermediate) {
    CHECK_ERROR_CODE(context->follow_page_pointer(
      hash_intermediate_volatile_page_init,
      !for_write,  // null page is a valid result only for reads ("not found")
      for_write,
      true,   // if we jump to snapshot page, we need to add it to pointer set for serializability.
      &pointer,
      page,
      reinterpret_cast<Page*>(parent),
      index_in_parent));
  } else {
    // we are in a level-0 volatile page. so the pointee is a bin-head.
    // we need a bit special handling in this case
    CHECK_ERROR_CODE(follow_page_bin_head(context, for_write, parent, index_in_parent, page));
  }

  if (*page) {
    if (child_intermediate) {
      ASSERT_ND((*page)->get_page_type() == kHashIntermediatePageType);
      ASSERT_ND((*page)->get_header().get_in_layer_level() + 1U == parent_level);
    } else {
      ASSERT_ND((*page)->get_page_type() == kHashDataPageType);
      ASSERT_ND(reinterpret_cast<HashDataPage*>(*page)->get_bin()
        == parent->get_bin_range().begin_ + index_in_parent);
    }
  }
  return kErrorCodeOk;
}

ErrorCode HashStoragePimpl::follow_page_bin_head(
  thread::Thread* context,
  bool for_write,
  HashIntermediatePage* parent,
  uint16_t index_in_parent,
  Page** page) {
  // do we have to newly create a volatile version of the pointed bin?
  ASSERT_ND(!parent->header().snapshot_);
  ASSERT_ND(parent->header().get_page_type() == kHashIntermediatePageType);
  ASSERT_ND(parent->get_level() == 0);
  xct::Xct& cur_xct = context->get_current_xct();
  xct::IsolationLevel isolation = cur_xct.get_isolation_level();
  DualPagePointer* pointer = parent->get_pointer_address(index_in_parent);

  // otherwise why in volatile page.
  ASSERT_ND(for_write || isolation != xct::kSnapshot || pointer->snapshot_pointer_ == 0);
  // in other words, we can safely "prefer" volatile page here.
  if (!pointer->volatile_pointer_.is_null()) {
    *page = context->resolve(pointer->volatile_pointer_);
  } else {
    SnapshotPagePointer snapshot_pointer = pointer->snapshot_pointer_;
    if (!for_write) {
      // reads don't have to create a new page. easy
      if (snapshot_pointer == 0) {
        *page = nullptr;
      } else {
        CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(snapshot_pointer, page));
      }

      if (isolation == xct::kSerializable) {
        VolatilePagePointer null_pointer;
        null_pointer.clear();
        cur_xct.add_to_pointer_set(&(pointer->volatile_pointer_), null_pointer);
      }
    } else {
      // writes need the volatile version.
      if (snapshot_pointer == 0) {
        // The bin is completely empty. we just make a new empty page.
        CHECK_ERROR_CODE(context->follow_page_pointer(
          hash_data_volatile_page_init,
          false,
          true,
          true,
          pointer,
          page,
          reinterpret_cast<Page*>(parent),
          index_in_parent));
      } else {
        // Otherwise, we must create a volatile version of the existing page.
        // a special rule for hash storage in this case: we create/drop volatile versions
        // in the granularity of hash bin. all or nothing.
        // thus, not just the head page of the bin, we have to volatilize the entire bin.
        memory::NumaCoreMemory* core_memory = context->get_thread_memory();
        const memory::LocalPageResolver& local_resolver
          = context->get_local_volatile_page_resolver();
        VolatilePagePointer head_page_id = core_memory->grab_free_volatile_page_pointer();
        const auto offset = head_page_id.get_offset();
        if (UNLIKELY(head_page_id.is_null())) {
          return kErrorCodeMemoryNoFreePages;
        }

        HashDataPage* head_page
          = reinterpret_cast<HashDataPage*>(local_resolver.resolve_offset_newpage(offset));
        storage::Page* snapshot_head;
        ErrorCode code = context->find_or_read_a_snapshot_page(snapshot_pointer, &snapshot_head);
        if (code != kErrorCodeOk) {
          core_memory->release_free_volatile_page(offset);
          return code;
        }

        std::memcpy(head_page, snapshot_head, kPageSize);
        ASSERT_ND(head_page->header().snapshot_);
        head_page->header().snapshot_ = false;
        head_page->header().page_id_ = head_page_id.word;

        // load following pages. hopefully this is a rare case.
        ErrorCode last_error = kErrorCodeOk;
        if (UNLIKELY(head_page->next_page().snapshot_pointer_)) {
          HashDataPage* cur_page = head_page;
          while (true) {
            ASSERT_ND(last_error == kErrorCodeOk);
            SnapshotPagePointer next = cur_page->next_page().snapshot_pointer_;
            if (next == 0) {
              break;
            }

            DVLOG(1) << "Following next-link in hash data pages. Hopefully it's not that long..";
            VolatilePagePointer next_page_id = core_memory->grab_free_volatile_page_pointer();
            memory::PagePoolOffset next_offset = next_page_id.get_offset();
            if (UNLIKELY(next_page_id.is_null())) {
              // we have to release preceding pages too
              last_error = kErrorCodeMemoryNoFreePages;
              break;
            }
            HashDataPage* next_page
              = reinterpret_cast<HashDataPage*>(local_resolver.resolve_offset_newpage(next_offset));
            // immediately install because:
            // 1) we don't have any race here, 2) we need to follow them to release on error.
            DualPagePointer* target = cur_page->next_page_address();
            ASSERT_ND(target->volatile_pointer_.is_null());
            target->volatile_pointer_ = next_page_id;
            target->snapshot_pointer_ = 0;  // will be no longer used, let's clear them

            storage::Page* snapshot_page;
            last_error = context->find_or_read_a_snapshot_page(next, &snapshot_page);
            if (last_error != kErrorCodeOk) {
              break;
            }
            std::memcpy(next_page, snapshot_page, kPageSize);
            ASSERT_ND(next_page->header().snapshot_);
            ASSERT_ND(next_page->get_bin() == cur_page->get_bin());
            next_page->header().snapshot_ = false;
            next_page->header().page_id_ = next_page_id.word;
            cur_page = next_page;
          }
        }

        // all rihgt, now atomically install the pointer to the volatile head page.
        bool must_release_pages = false;
        if (last_error == kErrorCodeOk) {
          uint64_t expected = 0;
          if (assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &(pointer->volatile_pointer_.word),
            &expected,
            head_page_id.word)) {
            // successfully installed the head pointer. fine.
            *page = reinterpret_cast<Page*>(head_page);
          } else {
            ASSERT_ND(expected);
            // someone else has installed it, which is also fine.
            // but, we must release pages we created (which turned out to be a waste effort)
            LOG(INFO) << "Interesting. Someone else has installed a volatile version.";
            *page = context->resolve(pointer->volatile_pointer_);
            must_release_pages = true;
          }
        } else {
          must_release_pages = true;
        }

        if (must_release_pages) {
          HashDataPage* cur = head_page;
          while (true) {
            VolatilePagePointer cur_id = construct_volatile_page_pointer(cur->header().page_id_);
            ASSERT_ND(cur_id.components.numa_node == context->get_numa_node());
            ASSERT_ND(!cur_id.is_null());
            // retrieve next_id BEFORE releasing (revoking) cur page.
            VolatilePagePointer next_id = cur->next_page().volatile_pointer_;
            core_memory->release_free_volatile_page(cur_id.get_offset());
            if (next_id.is_null()) {
              break;
            }
            cur = context->resolve_cast<HashDataPage>(next_id);
          }
        }

        CHECK_ERROR_CODE(last_error);
      }
    }
  }

  return kErrorCodeOk;
}

ErrorCode HashStoragePimpl::locate_bin(
  thread::Thread* context,
  bool for_write,
  const HashCombo& combo,
  HashDataPage** bin_head) {
  HashIntermediatePage* root;
  CHECK_ERROR_CODE(get_root_page(context, for_write, &root));
  ASSERT_ND(root);
  *bin_head = nullptr;
  xct::Xct& current_xct = context->get_current_xct();

  HashIntermediatePage* parent = root;
  while (true) {
    ASSERT_ND(parent);
    uint8_t parent_level = parent->get_level();
    uint16_t index = combo.route_.route[parent_level];
    Page* next;
    CHECK_ERROR_CODE(follow_page(context, for_write, parent, index, &next));
    if (!next) {
      // if this is a read-access, it is possible that the page even doesn't exist.
      // it is a valid result (not found). we just have to add a pointer set to protect the result.
      ASSERT_ND(!for_write);
      if (!parent->header().snapshot_  // then we already added a pointer set in higher level
        && current_xct.get_isolation_level() == xct::kSerializable) {
        VolatilePagePointer volatile_null;
        volatile_null.clear();
        CHECK_ERROR_CODE(
          current_xct.add_to_pointer_set(
            &parent->get_pointer(index).volatile_pointer_,
            volatile_null));
      }
      break;
    } else {
      if (parent_level == 0) {
        *bin_head = reinterpret_cast<HashDataPage*>(next);
        break;
      } else {
        parent = reinterpret_cast<HashIntermediatePage*>(next);
      }
    }
  }

  ASSERT_ND(*bin_head != nullptr || !for_write);
  ASSERT_ND(*bin_head == nullptr || (*bin_head)->get_bin() == combo.bin_);
  return kErrorCodeOk;
}

ErrorCode HashStoragePimpl::locate_record_in_snapshot(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  HashDataPage* bin_head,
  RecordLocation* result) {
  ASSERT_ND(bin_head);
  ASSERT_ND(bin_head->get_bin() == combo.bin_);
  ASSERT_ND(bin_head->header().snapshot_);
  result->clear();
  // Snapshot version doesn't need any of the concerns above. Easy!
  // Just physically search and follow to next page.
  HashDataPage* page = bin_head;
  while (true) {
    const uint16_t record_count = page->get_record_count();
    const DataPageSlotIndex index = page->search_key_physical(
      combo.hash_,
      combo.fingerprint_,
      key,
      key_length,
      record_count);
    if (index != kSlotNotFound) {
      // found! this is final in snapshot page, so physical-only suffices (no protection needed).
      ASSERT_ND(page->compare_slot_key(index, combo.hash_, key, key_length));
      result->populate_physical(page, index);
      ASSERT_ND(!result->observed_.is_moved());
      return kErrorCodeOk;
    }

    // Definitely not in this page, now on to next page.
    DualPagePointer* next_page = page->next_page_address();
    // then we are in snapshot world. no race.
    ASSERT_ND(next_page->volatile_pointer_.is_null());
    SnapshotPagePointer pointer = next_page->snapshot_pointer_;
    if (pointer) {
      Page* next;
      CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(pointer, &next));
      ASSERT_ND(next->get_header().snapshot_);
      page = reinterpret_cast<HashDataPage*>(next);
    } else {
      // it's snapshot world. the result is final, we are done.
      return kErrorCodeOk;
    }
  }
}

ErrorCode HashStoragePimpl::locate_record(
  thread::Thread* context,
  bool for_write,
  bool physical_only,
  bool create_if_notfound,
  uint16_t create_payload_length,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  HashDataPage* bin_head,
  RecordLocation* result) {
  ASSERT_ND(bin_head);
  ASSERT_ND(bin_head->get_bin() == combo.bin_);
  ASSERT_ND(for_write || !create_if_notfound);  // create_if_notfound implies for_write

  // Snapshot case is way easier. Separtely handle that case.
  if (bin_head->header().snapshot_) {
    ASSERT_ND(!for_write);
    ASSERT_ND(!create_if_notfound);
    return locate_record_in_snapshot(context, key, key_length, combo, bin_head, result);
  }

  xct::Xct* cur_xct = &context->get_current_xct();

  // in hash storage, we maintain only bin_head's stat. it's enough
  if (for_write) {
    bin_head->header().stat_last_updater_node_ = context->get_numa_node();
  }

  HashDataPage* page = bin_head;
  while (true) {
    // Start with a physical-only search. We will re-check our observation below.
    const uint16_t record_count = page->get_record_count();
    const DataPageSlotIndex index = page->search_key_physical(
      combo.hash_,
      combo.fingerprint_,
      key,
      key_length,
      record_count);
    if (index != kSlotNotFound) {
      // found! but it might be now being logically moved/deleted.
      ASSERT_ND(page->compare_slot_key(index, combo.hash_, key, key_length));
      if (physical_only) {
        // we don't care. the caller is responsible to do logical operation
        result->populate_physical(page, index);
        return kErrorCodeOk;
      } else {
        CHECK_ERROR_CODE(result->populate_logical(cur_xct, page, index, for_write));
        if (UNLIKELY(result->observed_.is_moved())) {
          LOG(INFO) << "Interesting. The record has been just moved";
          continue;
        }
        return kErrorCodeOk;
      }
    }

    // Apparently not in this page, now on to next page. We have to be a bit careful
    // in this case. Non-null next page means this page is already static, but we
    // have to make sure we confirmed it in a right order.
    DualPagePointer* next_page = page->next_page_address();

    // we are in volatile page, there might be a race!
    PageVersionStatus page_status = page->header().page_version_.status_;
    assorted::memory_fence_consume();  // from now on, page_status is the ground truth here.
    // check a few things after the fence.
    // invariant: we never move on to next page without guaranteeing that this page does not
    // contain a physical non-moved record with the key.

    // did someone insert a new record at this moment?
    uint16_t record_count_again = page->get_record_count();
    if (UNLIKELY(record_count != record_count_again)) {
      LOG(INFO) << "Interesting. concurrent insertion just happend to the page";
      assorted::memory_fence_consume();
      continue;  // just retry to make it sure. this is rare.
    }

    // did someone install a new page at this moment?
    if (UNLIKELY(!page_status.has_next_page() && !next_page->volatile_pointer_.is_null())) {
      LOG(INFO) << "Interesting. concurrent next-page installation just happend to the page";
      assorted::memory_fence_consume();
      continue;  // just retry to make it sure. this is rare.
    }

    if (next_page->volatile_pointer_.is_null()) {
      // no next page.
      if (create_if_notfound) {
        // this is the tail page, so let's insert it here.
        // we do that as a system transaction.
        ASSERT_ND(for_write);
        DataPageSlotIndex new_location;
        CHECK_ERROR_CODE(locate_record_reserve_physical(
          context,
          key,
          key_length,
          combo,
          create_payload_length,
          &page,
          record_count,
          &new_location));
        ASSERT_ND(new_location != kSlotNotFound);  // contract of the above method
        // The returned location is not logically protected... yet.
        ASSERT_ND(page->compare_slot_key(new_location, combo.hash_, key, key_length));
        if (physical_only) {
          // we don't care. the caller is responsible to do logical operation
          result->populate_physical(page, new_location);
          return kErrorCodeOk;
        } else {
          CHECK_ERROR_CODE(result->populate_logical(cur_xct, page, new_location, for_write));
          if (UNLIKELY(result->observed_.is_moved())) {
            LOG(INFO) << "Interesting. The record has been just moved after creation!";
            continue;
          }
          return kErrorCodeOk;
        }
      } else {
        // "NotFound" result. To finalize it,
        // we have to take version set because someone might
        // insert a new record/next-page later.
        result->clear();
        if (physical_only) {
          // we don't care. the caller is responsible to do logical operation
          return kErrorCodeOk;
        } else {
          // [Logical check]: Remember the page_status we observed as of checking record count
          // and verify it at commit time.
          CHECK_ERROR_CODE(cur_xct->add_to_page_version_set(
            &page->header().page_version_,
            page_status));
          return kErrorCodeOk;
        }
      }
    } else {
      page = context->resolve_cast<HashDataPage>(next_page->volatile_pointer_);
      ASSERT_ND(!page->header().snapshot_);
    }
  }
}

ErrorCode HashStoragePimpl::locate_record_reserve_physical(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  uint16_t payload_length,
  HashDataPage** page_in_out,
  uint16_t examined_records,
  DataPageSlotIndex* new_location) {
  ASSERT_ND(new_location);

  HashDataPage* page = *page_in_out;
  ASSERT_ND(!page->header().snapshot_);
#ifndef NDEBUG
  // examined_records is the count of records the caller has examined
  // _and_ found no matching records. Because keys in our hash pages are immutable,
  // there still must not be a record of the given key up to that slot.
  ASSERT_ND(examined_records <= page->get_record_count());
  DataPageSlotIndex re_examined = page->search_key_physical(
      combo.hash_,
      combo.fingerprint_,
      key,
      key_length,
      examined_records);  // do not check newer records
  ASSERT_ND(re_examined == kSlotNotFound);
#endif  // NDEBUG

  *new_location = kSlotNotFound;
  while (true) {
    // lock the page first so that there is no race on new keys.
    PageVersionLockScope scope(context, &page->header().page_version_);
    if (UNLIKELY(page->get_record_count() != examined_records)) {
      // oh, someone has just inserted something. let's check it again
      DVLOG(0) << "Interesting, there is a new record after locking the page.";
      uint16_t new_count = page->get_record_count();
      ASSERT_ND(new_count > examined_records);

      // in this case, we can skip the first examined_records records, but
      // this is rare. let's scan it again and do sanity check

      // We took the page lock, so physical-only search is enough.
      DataPageSlotIndex index = page->search_key_physical(
        combo.hash_,
        combo.fingerprint_,
        key,
        key_length,
        new_count);
      if (index != kSlotNotFound) {
        // the found slot must be AFTER examined_records, otherwise we should have found it earlier.
        LOG(INFO) << "Interesting, the key has been just inserted!";
        ASSERT_ND(index >= examined_records);
        *new_location = index;
        return kErrorCodeOk;
      } else {
        // still no match, go on.
        examined_records = new_count;
      }
    }

    if (!page->next_page().volatile_pointer_.is_null()) {
      DVLOG(0) << "Interesting, there is a new next page after locking the page.";
      page = context->resolve_cast<HashDataPage>(page->next_page().volatile_pointer_);
      *page_in_out = page;
      examined_records = 0;
      ASSERT_ND(!page->header().snapshot_);
      continue;
    }

    // do we have enough room in this page?
    uint16_t available_space = page->available_space();
    uint16_t required_space = HashDataPage::required_space(key_length, payload_length);
    if (available_space < required_space) {
      HashDataPage* next;
      CHECK_ERROR_CODE(append_next_volatile_page(context, page, &scope, &next));

      // just goes on to the newly created next page
      page = next;
      *page_in_out = page;
      examined_records = 0;
      ASSERT_ND(!page->header().snapshot_);
      continue;
    }

    // the page is enough spacious, and has no next page. we rule!
    DataPageSlotIndex index = page->reserve_record(
      combo.hash_,
      combo.fingerprint_,
      key,
      key_length,
      payload_length);
    ASSERT_ND(index == examined_records);
    *new_location = index;
    return kErrorCodeOk;
  }
}

xct::TrackMovedRecordResult HashStoragePimpl::track_moved_record(
  xct::RwLockableXctId* old_address,
  xct::WriteXctAccess* write_set) {
  ASSERT_ND(old_address);
  ASSERT_ND(old_address->is_moved());
  // We use moved bit only for volatile data pages
  HashDataPage* page = reinterpret_cast<HashDataPage*>(to_page(old_address));
  ASSERT_ND(!page->header().snapshot_);
  ASSERT_ND(page->header().get_page_type() == kHashDataPageType);

  // TID is the first member in slot, so this ugly cast works.
  HashDataPage::Slot* old_slot = reinterpret_cast<HashDataPage::Slot*>(old_address);
  ASSERT_ND(&old_slot->tid_ == old_address);

  // for tracking, we need the full key and hash. let's extract them.
  const char* key = page->record_from_offset(old_slot->offset_);
  uint16_t key_length = old_slot->key_length_;
  HashCombo combo(key, key_length, control_block_->meta_);

  // we need write_set only for sanity check. It's easier in hash storage!
  if (write_set) {
#ifndef NDEBUG
    ASSERT_ND(write_set->storage_id_ == page->header().storage_id_);
    ASSERT_ND(write_set->payload_address_ == page->record_from_offset(old_slot->offset_));
    HashCommonLogType* the_log = reinterpret_cast<HashCommonLogType*>(write_set->log_entry_);
    the_log->assert_record_and_log_keys(old_address, page->record_from_offset(old_slot->offset_));
#endif  // NDEBUG
  }
  HashDataPage::Slot* slot_origin = reinterpret_cast<HashDataPage::Slot*>(page + 1);
  ASSERT_ND(slot_origin > old_slot);  // because origin corresponds to "-1".
  DataPageSlotIndex old_index = slot_origin - old_slot - 1;
  ASSERT_ND(page->get_slot_address(old_index) == old_slot);

  return track_moved_record_search(page, key, key_length, combo);
}

xct::TrackMovedRecordResult HashStoragePimpl::track_moved_record_search(
  HashDataPage* page,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo) {
  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  RecordLocation result;
  while (true) {
    ASSERT_ND(!page->header().snapshot_);
    ASSERT_ND(page->next_page_address()->snapshot_pointer_ == 0);
    const uint16_t record_count = page->get_record_count();

    // Tracking happens in commit phase, so we don't need further logical readset/lock things.
    // This is just to locate the new address. So, physical_only search.
    // (which might miss a new record being inserted, but it's just a bit conservative abort)
    DataPageSlotIndex index = page->search_key_physical(
      combo.hash_,
      combo.fingerprint_,
      key,
      key_length,
      record_count);
    if (index != kSlotNotFound) {
      HashDataPage::Slot* slot = page->get_slot_address(index);
      char* payload = page->record_from_offset(slot->offset_);
      return xct::TrackMovedRecordResult(&slot->tid_, payload);
    }

    // we must meet the same invariant as usual case. a bit simpler, though
    assorted::memory_fence_consume();
    DualPagePointer* next_page = page->next_page_address();
    assorted::memory_fence_consume();

    uint16_t record_count_again = page->get_record_count();
    if (UNLIKELY(record_count != record_count_again)) {
      LOG(INFO) << "Interesting. concurrent insertion just happend to the page";
      assorted::memory_fence_consume();
      continue;
    }
    if (next_page->volatile_pointer_.is_null()) {
      // This shouldn't happen as far as we flip moved bit after installing the new record
      LOG(WARNING) << "no next page?? but we didn't find the moved record in this page";
      assorted::memory_fence_acquire();
      if (next_page->volatile_pointer_.is_null()) {
        LOG(ERROR) << "Unexpected error, failed to track moved record in hash storage."
          << " This should not happen. hash combo=" << combo;
        return xct::TrackMovedRecordResult();
      }
      continue;
    }
    page = reinterpret_cast<HashDataPage*>(resolver.resolve_offset(next_page->volatile_pointer_));
  }
}

ErrorCode HashStoragePimpl::migrate_record_physical(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  HashDataPage* cur_page,
  DataPageSlotIndex cur_index,
  uint16_t payload_count) {
  ASSERT_ND(!cur_page->header().snapshot_);
  ASSERT_ND(cur_page->compare_slot_key(cur_index, combo.hash_, key, key_length));
  HashDataPage::Slot* cur_slot = cur_page->get_slot_address(cur_index);

  // This is a lock on a single page, and we don't take any other lock here.
  // Thus no risk of deadlocks even in HCC.
  PageVersionLockScope cur_page_scope(context, &cur_page->header().page_version_);
  // However, the record lock might be risking deadlock. We thus release in-flight locks first.
  // After here, cur_page will not have a new entry.
  context->mcs_release_all_current_locks_at_and_after(
    xct::xct_id_to_universal_lock_id(
      context->get_global_volatile_page_resolver(), &cur_slot->tid_));
  xct::McsRwLockScope cur_record_scope(context, &cur_slot->tid_, false, true, false);
  // cur_slot's status is now finalized.
  if (cur_slot->tid_.is_moved()) {
    // rare, but possible. locate the current record then.
    VLOG(0) << "Interesting. Concurrent thread has already migrated this record!";
    ASSERT_ND(cur_page->compare_slot_key(cur_index, combo.hash_, key, key_length));

    // corresponds to rel-barrier while setting is_moved flag.
    assorted::memory_fence_acquire();  // could be consume, but whatever
    RecordLocation new_location;
    CHECK_ERROR_CODE(locate_record_physical_only(
      context,
      true,
      false,
      payload_count,
      key,
      key_length,
      combo,
      cur_page,
      &new_location));
    // Thanks to the barrier above, we are sure there is a new record location.
    ASSERT_ND(new_location.is_found());
    // note: the new_location might be still is_moved, but that's not what this method is
    // responsible for. caller will retry. to make it sure, we need to hold more than one lock,
    // not worth it here. this method is not called that frequently.
    return kErrorCodeOk;
  }

  // now we are sure the current record is NOT moved, thus there is no chance that
  // this hash bucket has any other non-moved record of this exact key.
  // Just append to the tail! Where is the tail?
  ASSERT_ND(!cur_slot->tid_.is_moved());
  const uint16_t required_space = HashDataPage::required_space(key_length, payload_count);
  if (cur_page->next_page().volatile_pointer_.is_null()) {
    if (cur_page->available_space() >= required_space) {
      // Appending to the same page.
      // We treat this case separately because we don't have to lock another page.
      VLOG(2) << "Migrating to the same page. Faster.";
      migrate_record_move_physical(
        key,
        key_length,
        combo,
        cur_page,
        cur_index,
        cur_page,
        payload_count);
      return kErrorCodeOk;
    } else {
      VLOG(2) << "Needs to make a next page to the current page.";
      // we already hold a page lock, so we can safely add a next page
      HashDataPage* next;
      CHECK_ERROR_CODE(append_next_volatile_page(context, cur_page, &cur_page_scope, &next));
    }
  }

  // now we are sure that we are moving the record to another page.
  VLOG(2) << "Migrating to another page.";
  ASSERT_ND(!cur_page->next_page().volatile_pointer_.is_null());
  HashDataPage* tail_page = context->resolve_cast<HashDataPage>(
    cur_page->next_page().volatile_pointer_);
  while (true) {
    ASSERT_ND(tail_page);
    ASSERT_ND(tail_page != cur_page);
    VolatilePagePointer pointer = tail_page->next_page().volatile_pointer_;
    if (!pointer.is_null()) {
      tail_page = context->resolve_cast<HashDataPage>(pointer);
      continue;
    } else {
      // okay, looks like this is the tail page.. but we have to make sure after taking page lock.
      // We anyway need to take a page lock below, so do it now.
      PageVersionLockScope tail_page_scope(context, &tail_page->header().page_version_);
      if (!tail_page->next_page().volatile_pointer_.is_null()) {
        VLOG(0) << "Interesting. Someone else has just added next page";
        continue;
      }

      if (tail_page->available_space() >= required_space) {
        migrate_record_move_physical(
          key,
          key_length,
          combo,
          cur_page,
          cur_index,
          tail_page,
          payload_count);
        return kErrorCodeOk;
      } else {
        VLOG(2) << "Needs to make a next page to the tail page.";
        HashDataPage* next;
        CHECK_ERROR_CODE(append_next_volatile_page(context, tail_page, &tail_page_scope, &next));
        tail_page = next;
        continue;
      }
    }
  }


  return kErrorCodeOk;
}

void HashStoragePimpl::migrate_record_move_physical(
  const void* key,
  uint16_t key_length,
  const HashCombo& combo,
  HashDataPage* cur_page,
  DataPageSlotIndex cur_index,
  HashDataPage* tail_page,
  uint16_t payload_count) {
  ASSERT_ND(cur_page->header().page_version_.is_locked());
  ASSERT_ND(tail_page->header().page_version_.is_locked());
  ASSERT_ND(tail_page->next_page().volatile_pointer_.is_null());
  const uint16_t required_space = HashDataPage::required_space(key_length, payload_count);
  HashDataPage::Slot* cur_slot = cur_page->get_slot_address(cur_index);
  ASSERT_ND(tail_page->available_space() >= required_space);

  DataPageSlotIndex new_index = tail_page->reserve_record(
    combo.hash_,
    combo.fingerprint_,
    key,
    key_length,
    payload_count);
  HashDataPage::Slot* new_slot = tail_page->get_slot_address(new_index);

  // Here, we avoid locking this new record for ease of deadlock handling in HCC.
  // As this is a new record, no need to take a lock.
  xct::XctId new_xct_id = cur_slot->tid_.xct_id_;
  new_slot->tid_.xct_id_ = new_xct_id;

  assorted::memory_fence_release();  // see migrate_record_physical() for why we need it.
  // well, actually the lock in new_record_scope already does it, but for easier understanding..
  cur_slot->tid_.xct_id_.set_moved();
  assorted::memory_fence_acq_rel();  // to ease the caller.
}

ErrorCode HashStoragePimpl::append_next_volatile_page(
  thread::Thread* context,
  HashDataPage* page,
  PageVersionLockScope* scope,
  HashDataPage** next_page) {
  ASSERT_ND(page->header().page_version_.is_locked());
  ASSERT_ND(scope->version_ == &page->header().page_version_);
  ASSERT_ND(!scope->released_);
  ASSERT_ND(page->header().storage_id_ == get_id());
  ASSERT_ND(!page->header().snapshot_);
  ASSERT_ND(page->get_record_count() > 0);

  DVLOG(2) << "Volatile HashDataPage is full. Adding a next page..";
  VolatilePagePointer new_pointer = context->get_thread_memory()->grab_free_volatile_page_pointer();
  if (UNLIKELY(new_pointer.is_null())) {
    *next_page = nullptr;
    return kErrorCodeMemoryNoFreePages;
  }

  HashDataPage* next = context->resolve_newpage_cast<HashDataPage>(new_pointer.get_offset());
  HashBin bin = page->get_bin();
  next->initialize_volatile_page(
    get_id(),
    new_pointer,
    reinterpret_cast<Page*>(page),
    bin,
    get_bin_bits(),
    get_bin_shifts());
  assorted::memory_fence_release();  // so that others don't see uninitialized page
  page->next_page().volatile_pointer_ = new_pointer;
  assorted::memory_fence_release();  // so that others don't have "where's the next page" issue
  page->header().page_version_.set_has_next_page();
  scope->set_changed();

  // just goes on to the newly created next page
  *next_page = next;
  return kErrorCodeOk;
}

// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE

#define EXPIN_5I(x) template ErrorCode HashStoragePimpl::increment_record< x > \
  (thread::Thread* context, \
  const void* key, \
  uint16_t key_length, \
  const HashCombo& combo, \
  x* value, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5I);
// @endcond

}  // namespace hash
}  // namespace storage
}  // namespace foedus
