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
#include "foedus/storage/array/array_storage_pimpl.hpp"

#include <glog/logging.h>

#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "foedus/xct/xct_optimistic_read_impl.hpp"

namespace foedus {
namespace storage {
namespace array {
// FIXME(tzwang): overwrite_record here doesn't add to read set, should we?
// Defines ArrayStorage methods so that we can inline implementation calls
uint16_t    ArrayStorage::get_payload_size() const  { return control_block_->meta_.payload_size_; }
ArrayOffset ArrayStorage::get_array_size()   const  { return control_block_->meta_.array_size_; }
uint8_t     ArrayStorage::get_levels()       const  { return control_block_->levels_; }
const ArrayMetadata* ArrayStorage::get_array_metadata() const  { return &control_block_->meta_; }

ErrorStack ArrayStorage::verify_single_thread(thread::Thread* context) {
  return ArrayStoragePimpl(this).verify_single_thread(context);
}

ErrorCode ArrayStorage::get_record(
  thread::Thread* context, ArrayOffset offset, void *payload) {
  return get_record(context, offset, payload, 0, get_payload_size());
}

ErrorCode ArrayStorage::get_record(thread::Thread* context, ArrayOffset offset,
          void *payload, uint16_t payload_offset, uint16_t payload_count) {
  return ArrayStoragePimpl(this).get_record(
    context, offset, payload, payload_offset, payload_count);
}

template <typename T>
ErrorCode ArrayStorage::get_record_primitive(thread::Thread* context, ArrayOffset offset,
          T *payload, uint16_t payload_offset) {
  return ArrayStoragePimpl(this).get_record_primitive<T>(context, offset, payload, payload_offset);
}

ErrorCode ArrayStorage::get_record_payload(
  thread::Thread* context,
  ArrayOffset offset,
  const void **payload) {
  return ArrayStoragePimpl(this).get_record_payload(context, offset, payload);
}

ErrorCode ArrayStorage::get_record_for_write(
  thread::Thread* context,
  ArrayOffset offset,
  Record** record) {
  return ArrayStoragePimpl(this).get_record_for_write(context, offset, record);
}

ErrorCode ArrayStorage::overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) {
  return ArrayStoragePimpl(this).overwrite_record(
    context,
    offset,
    payload,
    payload_offset,
    payload_count);
}

template <typename T>
ErrorCode ArrayStorage::overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
          T payload, uint16_t payload_offset) {
  return ArrayStoragePimpl(this).overwrite_record_primitive<T>(
    context,
    offset,
    payload,
    payload_offset);
}

ErrorCode ArrayStorage::overwrite_record(
  thread::Thread* context,
  ArrayOffset offset,
  Record* record,
  const void *payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return ArrayStoragePimpl(this).overwrite_record(
    context,
    offset,
    record,
    payload,
    payload_offset,
    payload_count);
}

template <typename T>
ErrorCode ArrayStorage::overwrite_record_primitive(
  thread::Thread* context,
  ArrayOffset offset,
  Record* record,
  T payload,
  uint16_t payload_offset) {
  return ArrayStoragePimpl(this).overwrite_record_primitive<T>(
    context,
    offset,
    record,
    payload,
    payload_offset);
}

template <typename T>
ErrorCode ArrayStorage::increment_record(thread::Thread* context, ArrayOffset offset,
          T* value, uint16_t payload_offset) {
  return ArrayStoragePimpl(this).increment_record<T>(context, offset, value, payload_offset);
}

template <typename T>
ErrorCode ArrayStorage::increment_record_oneshot(
  thread::Thread* context,
  ArrayOffset offset,
  T value,
  uint16_t payload_offset) {
  return ArrayStoragePimpl(this).increment_record_oneshot<T>(
    context,
    offset,
    value,
    payload_offset);
}

/**
 * Calculate leaf/interior pages we need.
 * @return index=level.
 */
std::vector<uint64_t> ArrayStoragePimpl::calculate_required_pages(
  uint64_t array_size, uint16_t payload) {
  payload = assorted::align8(payload);
  uint64_t records_per_page = kDataSize / (payload + kRecordOverhead);

  // so, how many leaf pages do we need?
  uint64_t leaf_pages = assorted::int_div_ceil(array_size, records_per_page);
  LOG(INFO) << "We need " << leaf_pages << " leaf pages";

  // interior nodes
  uint64_t total_pages = leaf_pages;
  std::vector<uint64_t> pages;
  pages.push_back(leaf_pages);
  while (pages.back() != 1) {
    uint64_t next_level_pages = assorted::int_div_ceil(pages.back(), kInteriorFanout);
    LOG(INFO) << "Level-" << pages.size() << " would have " << next_level_pages << " pages";
    pages.push_back(next_level_pages);
    total_pages += next_level_pages;
  }

  LOG(INFO) << "In total, we need " << total_pages << " pages";
  return pages;
}

uint8_t calculate_levels(const ArrayMetadata &metadata) {
  uint64_t array_size = metadata.array_size_;
  uint16_t payload = assorted::align8(metadata.payload_size_);
  uint64_t records_per_page = kDataSize / (payload + kRecordOverhead);
  uint8_t levels = 1;
  for (uint64_t pages = assorted::int_div_ceil(array_size, records_per_page);
        pages != 1;
        pages = assorted::int_div_ceil(pages, kInteriorFanout)) {
    ++levels;
  }
  return levels;
}

void ArrayStoragePimpl::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& resolver,
  memory::PageReleaseBatch* batch,
  VolatilePagePointer volatile_page_id) {
  ASSERT_ND(volatile_page_id.components.offset != 0);
  ArrayPage* page = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(volatile_page_id));
  if (!page->is_leaf()) {
    for (uint16_t i = 0; i < kInteriorFanout; ++i) {
      DualPagePointer &child_pointer = page->get_interior_record(i);
      VolatilePagePointer child_page_id = child_pointer.volatile_pointer_;
      if (child_page_id.components.offset != 0) {
        // then recurse
        release_pages_recursive(resolver, batch, child_page_id);
        child_pointer.volatile_pointer_.word = 0;
      }
    }
  }
  batch->release(volatile_page_id);
}

ErrorStack ArrayStorage::drop() {
  LOG(INFO) << "Uninitializing an array-storage " << *this;
  if (!control_block_->root_page_pointer_.volatile_pointer_.is_null()) {
    memory::PageReleaseBatch release_batch(engine_);
    ArrayStoragePimpl::release_pages_recursive(
      engine_->get_memory_manager()->get_global_volatile_page_resolver(),
      &release_batch,
      control_block_->root_page_pointer_.volatile_pointer_);
    release_batch.release_all();
    control_block_->root_page_pointer_.volatile_pointer_.word = 0;
  }
  return kRetOk;
}

std::vector<uint64_t> ArrayStoragePimpl::calculate_offset_intervals(
  uint8_t levels,
  uint16_t payload) {
  const uint16_t payload_size_aligned = (assorted::align8(payload));

  std::vector<uint64_t> offset_intervals;
  offset_intervals.push_back(kDataSize / (payload_size_aligned + kRecordOverhead));
  for (uint8_t level = 1; level < levels; ++level) {
    offset_intervals.push_back(offset_intervals[level - 1] * kInteriorFanout);
  }
  return offset_intervals;
}

ErrorStack ArrayStoragePimpl::load_empty() {
  const uint16_t levels = calculate_levels(control_block_->meta_);
  const uint32_t payload_size = control_block_->meta_.payload_size_;
  const ArrayOffset array_size = control_block_->meta_.array_size_;
  if (array_size > kMaxArrayOffset) {
    return ERROR_STACK(kErrorCodeStrTooLargeArray);
  }
  control_block_->levels_ = levels;
  control_block_->route_finder_ = LookupRouteFinder(levels, payload_size);
  control_block_->root_page_pointer_.snapshot_pointer_ = 0;
  control_block_->root_page_pointer_.volatile_pointer_.word = 0;
  control_block_->meta_.root_snapshot_page_id_ = 0;
  control_block_->intervals_[0] = control_block_->route_finder_.get_records_in_leaf();
  for (uint16_t level = 1; level < levels; ++level) {
    control_block_->intervals_[level] = control_block_->intervals_[level - 1U] * kInteriorFanout;
  }

  VolatilePagePointer volatile_pointer;
  ArrayPage* volatile_root;
  CHECK_ERROR(engine_->get_memory_manager()->grab_one_volatile_page(
    0,
    &volatile_pointer,
    reinterpret_cast<Page**>(&volatile_root)));
  volatile_root->initialize_volatile_page(
    engine_->get_savepoint_manager()->get_initial_current_epoch(),  // lowest epoch in the system
    get_id(),
    volatile_pointer,
    payload_size,
    levels - 1U,
    ArrayRange(0, array_size));
  control_block_->root_page_pointer_.volatile_pointer_ = volatile_pointer;
  return kRetOk;
}

ErrorStack ArrayStoragePimpl::create(const Metadata& metadata) {
  if (exists()) {
    LOG(ERROR) << "This array-storage already exists-" << metadata.id_;
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }
  control_block_->meta_ = static_cast<const ArrayMetadata&>(metadata);
  CHECK_ERROR(load_empty());
  LOG(INFO) << "Newly created an array-storage " << metadata.id_;

  control_block_->status_ = kExists;
  return kRetOk;
}

ErrorStack ArrayStoragePimpl::load(const StorageControlBlock& snapshot_block) {
  control_block_->meta_ = static_cast<const ArrayMetadata&>(snapshot_block.meta_);
  const ArrayMetadata& meta = control_block_->meta_;
  ASSERT_ND(meta.root_snapshot_page_id_ != 0);
  const uint16_t levels = calculate_levels(meta);
  control_block_->levels_ = levels;
  control_block_->route_finder_ = LookupRouteFinder(levels, get_payload_size());
  control_block_->root_page_pointer_.snapshot_pointer_ = meta.root_snapshot_page_id_;
  control_block_->root_page_pointer_.volatile_pointer_.word = 0;

  // So far we assume the root page always has a volatile version.
  // Create it now.
  if (meta.root_snapshot_page_id_ != 0) {
    cache::SnapshotFileSet fileset(engine_);
    CHECK_ERROR(fileset.initialize());
    UninitializeGuard fileset_guard(&fileset, UninitializeGuard::kWarnIfUninitializeError);
    VolatilePagePointer volatile_pointer;
    Page* volatile_root;
    CHECK_ERROR(engine_->get_memory_manager()->load_one_volatile_page(
      &fileset,
      meta.root_snapshot_page_id_,
      &volatile_pointer,
      &volatile_root));
    control_block_->root_page_pointer_.volatile_pointer_ = volatile_pointer;
    CHECK_ERROR(fileset.uninitialize());
  } else {
    LOG(INFO) << "Loading an empty array-storage-" << get_meta();
    CHECK_ERROR(load_empty());
  }

  control_block_->status_ = kExists;
  LOG(INFO) << "Loaded an array-storage-" << get_meta();
  return kRetOk;
}


inline ErrorCode ArrayStoragePimpl::locate_record_for_read(
  thread::Thread* context,
  ArrayOffset offset,
  Record** out,
  bool* snapshot_record) {
  ASSERT_ND(exists());
  ASSERT_ND(offset < get_array_size());
  uint16_t index = 0;
  ArrayPage* page = nullptr;
  CHECK_ERROR_CODE(lookup_for_read(context, offset, &page, &index, snapshot_record));
  ASSERT_ND(page);
  ASSERT_ND(page->is_leaf());
  ASSERT_ND(page->get_array_range().contains(offset));
  ASSERT_ND(page->get_leaf_record(0, get_payload_size())->owner_id_.xct_id_.is_valid());
  *out = page->get_leaf_record(index, get_payload_size());
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::locate_record_for_write(
  thread::Thread* context,
  ArrayOffset offset,
  Record** out) {
  ASSERT_ND(exists());
  ASSERT_ND(offset < get_array_size());
  uint16_t index = 0;
  ArrayPage* page = nullptr;
  CHECK_ERROR_CODE(lookup_for_write(context, offset, &page, &index));
  ASSERT_ND(page);
  ASSERT_ND(page->is_leaf());
  ASSERT_ND(page->get_array_range().contains(offset));
  ASSERT_ND(page->get_leaf_record(0, get_payload_size())->owner_id_.xct_id_.is_valid());
  *out = page->get_leaf_record(index, get_payload_size());
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::get_record(
  thread::Thread* context,
  ArrayOffset offset,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  ASSERT_ND(payload_offset + payload_count <= get_payload_size());
  Record *record = nullptr;
  bool snapshot_record;
  CHECK_ERROR_CODE(locate_record_for_read(context, offset, &record, &snapshot_record));
  CHECK_ERROR_CODE(context->get_current_xct().on_record_read(false, &record->owner_id_));
  std::memcpy(payload, record->payload_ + payload_offset, payload_count);
  return kErrorCodeOk;
}

template <typename T>
ErrorCode ArrayStoragePimpl::get_record_primitive(
  thread::Thread* context,
  ArrayOffset offset,
  T *payload,
  uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= get_payload_size());
  Record *record = nullptr;
  bool snapshot_record;
  CHECK_ERROR_CODE(locate_record_for_read(context, offset, &record, &snapshot_record));
  CHECK_ERROR_CODE(context->get_current_xct().on_record_read(false, &record->owner_id_));
  char* ptr = record->payload_ + payload_offset;
  *payload = *reinterpret_cast<const T*>(ptr);
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::get_record_payload(
  thread::Thread* context,
  ArrayOffset offset,
  const void** payload) {
  Record *record = nullptr;
  bool snapshot_record;
  CHECK_ERROR_CODE(locate_record_for_read(context, offset, &record, &snapshot_record));
  xct::Xct& current_xct = context->get_current_xct();
  if (!snapshot_record &&
    current_xct.get_isolation_level() != xct::kDirtyRead) {
    CHECK_ERROR_CODE(current_xct.on_record_read(false, &record->owner_id_));
  }
  *payload = record->payload_;
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::get_record_for_write(
  thread::Thread* context,
  ArrayOffset offset,
  Record** record) {
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, record));
  xct::Xct& current_xct = context->get_current_xct();
  if (current_xct.get_isolation_level() != xct::kDirtyRead) {
    CHECK_ERROR_CODE(current_xct.on_record_read(true, &(*record)->owner_id_));
  }
  return kErrorCodeOk;
}


inline ErrorCode ArrayStoragePimpl::overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) {
  ASSERT_ND(payload_offset + payload_count <= get_payload_size());
  Record *record = nullptr;
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, &record));
  return overwrite_record(context, offset, record, payload, payload_offset, payload_count);
}

template <typename T>
ErrorCode ArrayStoragePimpl::overwrite_record_primitive(
      thread::Thread* context, ArrayOffset offset, T payload, uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= get_payload_size());
  Record *record = nullptr;
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, &record));
  return overwrite_record_primitive<T>(context, offset, record, payload, payload_offset);
}

inline ErrorCode ArrayStoragePimpl::overwrite_record(
  thread::Thread* context,
  ArrayOffset offset,
  Record* record,
  const void *payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  uint16_t log_length = ArrayOverwriteLogType::calculate_log_length(payload_count);
  ArrayOverwriteLogType* log_entry = reinterpret_cast<ArrayOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), offset, payload, payload_offset, payload_count);
  return context->get_current_xct().add_to_write_set(
    get_id(),
    &record->owner_id_,
    record->payload_,
    log_entry);
}

template <typename T>
inline ErrorCode ArrayStoragePimpl::overwrite_record_primitive(
  thread::Thread* context,
  ArrayOffset offset,
  Record* record,
  T payload,
  uint16_t payload_offset) {
  uint16_t log_length = ArrayOverwriteLogType::calculate_log_length(sizeof(T));
  ArrayOverwriteLogType* log_entry = reinterpret_cast<ArrayOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate_primitive<T>(get_id(), offset, payload, payload_offset);
  return context->get_current_xct().add_to_write_set(
    get_id(),
    &record->owner_id_,
    record->payload_,
    log_entry);
}

template <typename T>
ErrorCode ArrayStoragePimpl::increment_record(
      thread::Thread* context, ArrayOffset offset, T* value, uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= get_payload_size());
  Record *record = nullptr;
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, &record));

  // this is get_record + overwrite_record
  // NOTE This version is like other storage's increment implementation.
  // Taking read-set (and potentially locks), read the value, then remember the overwrite log.
  // However the increment_record_oneshot() below is pretty different.
  CHECK_ERROR_CODE(context->get_current_xct().on_record_read(true, &record->owner_id_));
  char* ptr = record->payload_ + payload_offset;
  T tmp = *reinterpret_cast<const T*>(ptr);
  *value += tmp;
  uint16_t log_length = ArrayOverwriteLogType::calculate_log_length(sizeof(T));
  ArrayOverwriteLogType* log_entry = reinterpret_cast<ArrayOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate_primitive<T>(get_id(), offset, *value, payload_offset);
  return context->get_current_xct().add_to_write_set(
    get_id(),
    &record->owner_id_,
    record->payload_,
    log_entry);
}

template <typename T>
ErrorCode ArrayStoragePimpl::increment_record_oneshot(
  thread::Thread* context,
  ArrayOffset offset,
  T value,
  uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= get_payload_size());
  Record *record = nullptr;
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, &record));
  // Only Array's increment can be the rare "write-set only" log.
  // other increments have to check deletion bit at least.
  // To make use of it, we do have array increment log with primitive type as parameter.
  ValueType type = to_value_type<T>();
  uint16_t log_length = ArrayIncrementLogType::calculate_log_length(type);
  ArrayIncrementLogType* log_entry = reinterpret_cast<ArrayIncrementLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate<T>(get_id(), offset, value, payload_offset);
  return context->get_current_xct().add_to_write_set(
    get_id(),
    &record->owner_id_,
    record->payload_,
    log_entry);
}

inline ErrorCode ArrayStoragePimpl::lookup_for_read(
  thread::Thread* context,
  ArrayOffset offset,
  ArrayPage** out,
  uint16_t* index,
  bool* snapshot_page) {
  ASSERT_ND(exists());
  ASSERT_ND(offset < get_array_size());
  ASSERT_ND(out);
  ASSERT_ND(index);
  ArrayPage* current_page;
  CHECK_ERROR_CODE(get_root_page(context, false, &current_page));
  uint16_t levels = get_levels();
  ASSERT_ND(current_page->get_array_range().contains(offset));
  LookupRoute route = control_block_->route_finder_.find_route(offset);
  bool in_snapshot = current_page->header().snapshot_;
  for (uint8_t level = levels - 1; level > 0; --level) {
    ASSERT_ND(current_page->get_array_range().contains(offset));
    DualPagePointer& pointer = current_page->get_interior_record(route.route[level]);
    CHECK_ERROR_CODE(follow_pointer(
      context,
      in_snapshot,
      false,
      &pointer,
      &current_page,
      current_page,
      route.route[level]));
    in_snapshot = current_page->header().snapshot_;
  }
  ASSERT_ND(current_page->is_leaf());
  ASSERT_ND(current_page->get_array_range().contains(offset));
  ASSERT_ND(current_page->get_array_range().begin_ + route.route[0] == offset);
  ASSERT_ND(current_page->get_leaf_record(0, get_payload_size())->owner_id_.xct_id_.is_valid());
  *out = current_page;
  *index = route.route[0];
  *snapshot_page = (*out)->header().snapshot_;
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::lookup_for_write(
  thread::Thread* context,
  ArrayOffset offset,
  ArrayPage** out,
  uint16_t* index) {
  ASSERT_ND(exists());
  ASSERT_ND(offset < get_array_size());
  ASSERT_ND(out);
  ASSERT_ND(index);
  ArrayPage* current_page;
  CHECK_ERROR_CODE(get_root_page(context, true, &current_page));
  ASSERT_ND(!current_page->header().snapshot_);
  uint16_t levels = get_levels();
  ASSERT_ND(current_page->get_array_range().contains(offset));
  LookupRoute route = control_block_->route_finder_.find_route(offset);
  for (uint8_t level = levels - 1; level > 0; --level) {
    ASSERT_ND(current_page->get_array_range().contains(offset));
    CHECK_ERROR_CODE(follow_pointer(
      context,
      false,
      true,
      &current_page->get_interior_record(route.route[level]),
      &current_page,
      current_page,
      route.route[level]));
  }
  ASSERT_ND(current_page->is_leaf());
  ASSERT_ND(current_page->get_array_range().contains(offset));
  ASSERT_ND(current_page->get_array_range().begin_ + route.route[0] == offset);
  ASSERT_ND(current_page->get_leaf_record(0, get_payload_size())->owner_id_.xct_id_.is_valid());
  *out = current_page;
  *index = route.route[0];
  return kErrorCodeOk;
}

// so far experimental...
template <typename T>
ErrorCode ArrayStorage::get_record_primitive_batch(
  thread::Thread* context,
  uint16_t payload_offset,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  T* payload_batch) {
  ArrayStoragePimpl pimpl(this);
  for (uint16_t cur = 0; cur < batch_size;) {
    uint16_t chunk = batch_size - cur;
    if (chunk > ArrayStoragePimpl::kBatchMax) {
      chunk = ArrayStoragePimpl::kBatchMax;
    }
    CHECK_ERROR_CODE(pimpl.get_record_primitive_batch(
      context,
      payload_offset,
      chunk,
      &offset_batch[cur],
      &payload_batch[cur]));
    cur += chunk;
  }
  return kErrorCodeOk;
}

ErrorCode ArrayStorage::get_record_payload_batch(
  thread::Thread* context,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  const void** payload_batch) {
  ArrayStoragePimpl pimpl(this);
  for (uint16_t cur = 0; cur < batch_size;) {
    uint16_t chunk = batch_size - cur;
    if (chunk > ArrayStoragePimpl::kBatchMax) {
      chunk = ArrayStoragePimpl::kBatchMax;
    }
    CHECK_ERROR_CODE(pimpl.get_record_payload_batch(
      context,
      chunk,
      &offset_batch[cur],
      &payload_batch[cur]));
    cur += chunk;
  }
  return kErrorCodeOk;
}

ErrorCode ArrayStorage::get_record_for_write_batch(
  thread::Thread* context,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  Record** record_batch) {
  ArrayStoragePimpl pimpl(this);
  for (uint16_t cur = 0; cur < batch_size;) {
    uint16_t chunk = batch_size - cur;
    if (chunk > ArrayStoragePimpl::kBatchMax) {
      chunk = ArrayStoragePimpl::kBatchMax;
    }
    CHECK_ERROR_CODE(pimpl.get_record_for_write_batch(
      context,
      chunk,
      &offset_batch[cur],
      &record_batch[cur]));
    cur += chunk;
  }
  return kErrorCodeOk;
}


template <typename T>
inline ErrorCode ArrayStoragePimpl::get_record_primitive_batch(
  thread::Thread* context,
  uint16_t payload_offset,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  T* payload_batch) {
  ASSERT_ND(batch_size <= kBatchMax);
  Record* record_batch[kBatchMax];
  bool snapshot_record_batch[kBatchMax];
  CHECK_ERROR_CODE(locate_record_for_read_batch(
    context,
    batch_size,
    offset_batch,
    record_batch,
    snapshot_record_batch));
  xct::Xct& current_xct = context->get_current_xct();
  if (current_xct.get_isolation_level() != xct::kDirtyRead) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      if (!snapshot_record_batch[i]) {
        CHECK_ERROR_CODE(current_xct.on_record_read(false, &record_batch[i]->owner_id_));
      }
    }
    assorted::memory_fence_consume();
  }
  // we anyway prefetched the first 64 bytes. if the column is not within there,
  // let's do parallel prefetch
  if (payload_offset + sizeof(T) + kRecordOverhead > 64) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      assorted::prefetch_cacheline(record_batch[i]->payload_ + payload_offset);
    }
  }
  for (uint8_t i = 0; i < batch_size; ++i) {
    char* ptr = record_batch[i]->payload_ + payload_offset;
    payload_batch[i] = *reinterpret_cast<const T*>(ptr);
  }
  return kErrorCodeOk;
}


inline ErrorCode ArrayStoragePimpl::get_record_payload_batch(
  thread::Thread* context,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  const void** payload_batch) {
  ASSERT_ND(batch_size <= kBatchMax);
  Record* record_batch[kBatchMax];
  bool snapshot_record_batch[kBatchMax];
  CHECK_ERROR_CODE(locate_record_for_read_batch(
    context,
    batch_size,
    offset_batch,
    record_batch,
    snapshot_record_batch));
  xct::Xct& current_xct = context->get_current_xct();
  if (current_xct.get_isolation_level() != xct::kDirtyRead) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      if (!snapshot_record_batch[i]) {
        CHECK_ERROR_CODE(current_xct.on_record_read(false, &record_batch[i]->owner_id_));
      }
    }
    assorted::memory_fence_consume();
  }
  for (uint8_t i = 0; i < batch_size; ++i) {
    payload_batch[i] = record_batch[i]->payload_;
  }
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::get_record_for_write_batch(
  thread::Thread* context,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  Record** record_batch) {
  ASSERT_ND(batch_size <= kBatchMax);
  CHECK_ERROR_CODE(lookup_for_write_batch(
    context,
    batch_size,
    offset_batch,
    record_batch));
  xct::Xct& current_xct = context->get_current_xct();
  if (current_xct.get_isolation_level() != xct::kDirtyRead) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      CHECK_ERROR_CODE(current_xct.on_record_read(true, &record_batch[i]->owner_id_));
    }
    assorted::memory_fence_consume();
  }
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::locate_record_for_read_batch(
  thread::Thread* context,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  Record** out_batch,
  bool* snapshot_page_batch) {
  ASSERT_ND(batch_size <= kBatchMax);
  ArrayPage* page_batch[kBatchMax];
  uint16_t index_batch[kBatchMax];
  CHECK_ERROR_CODE(lookup_for_read_batch(
    context,
    batch_size,
    offset_batch,
    page_batch,
    index_batch,
    snapshot_page_batch));
  const uint16_t payload_size = get_payload_size();
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(page_batch[i]);
    ASSERT_ND(page_batch[i]->is_leaf());
    ASSERT_ND(page_batch[i]->get_array_range().contains(offset_batch[i]));
    out_batch[i] = page_batch[i]->get_leaf_record(index_batch[i], payload_size);
    ASSERT_ND(out_batch[i]->owner_id_.xct_id_.is_valid());
  }
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::lookup_for_read_batch(
  thread::Thread* context,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  ArrayPage** out_batch,
  uint16_t* index_batch,
  bool* snapshot_page_batch) {
  ASSERT_ND(batch_size <= kBatchMax);
  LookupRoute routes[kBatchMax];
  ArrayPage* root_page;
  CHECK_ERROR_CODE(get_root_page(context, false, &root_page));
  uint16_t levels = get_levels();
  bool root_snapshot = root_page->header().snapshot_;
  const uint16_t payload_size = get_payload_size();
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(offset_batch[i] < get_array_size());
    routes[i] = control_block_->route_finder_.find_route(offset_batch[i]);
    if (levels <= 1U) {
      assorted::prefetch_cacheline(root_page->get_leaf_record(
        routes[i].route[0],
        payload_size));
    } else {
      assorted::prefetch_cacheline(&root_page->get_interior_record(routes[i].route[levels - 1]));
    }
    out_batch[i] = root_page;
    snapshot_page_batch[i] = root_snapshot;
    ASSERT_ND(out_batch[i]->get_array_range().contains(offset_batch[i]));
    index_batch[i] = routes[i].route[levels - 1];
  }

  for (uint8_t level = levels - 1; level > 0; --level) {
    // note that we use out_batch as both input (parents) and output (the pages) here.
    // the method works in that case too.
    CHECK_ERROR_CODE(follow_pointers_for_read_batch(
      context,
      batch_size,
      snapshot_page_batch,
      out_batch,
      index_batch,
      out_batch));

    for (uint8_t i = 0; i < batch_size; ++i) {
      ASSERT_ND(snapshot_page_batch[i] == out_batch[i]->header().snapshot_);
      ASSERT_ND(out_batch[i]->get_storage_id() == get_id());
      ASSERT_ND(snapshot_page_batch[i]
        || !construct_volatile_page_pointer(out_batch[i]->header().page_id_).is_null());
      ASSERT_ND(!snapshot_page_batch[i]
        || extract_local_page_id_from_snapshot_pointer(out_batch[i]->header().page_id_));
      if (level == 1U) {
        assorted::prefetch_cacheline(out_batch[i]->get_leaf_record(
          routes[i].route[0],
          payload_size));
      } else {
        assorted::prefetch_cacheline(
          &out_batch[i]->get_interior_record(routes[i].route[levels - 1]));
      }
      index_batch[i] = routes[i].route[level - 1];
    }
  }
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(out_batch[i]->is_leaf());
    ASSERT_ND(out_batch[i]->get_array_range().contains(offset_batch[i]));
    ASSERT_ND(out_batch[i]->get_array_range().begin_ + routes[i].route[0] == offset_batch[i]);
    ASSERT_ND(index_batch[i] == routes[i].route[0]);
    ASSERT_ND(
      out_batch[i]->get_leaf_record(index_batch[i], payload_size)->owner_id_.xct_id_.is_valid());
  }
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::lookup_for_write_batch(
  thread::Thread* context,
  uint16_t batch_size,
  const ArrayOffset* offset_batch,
  Record** record_batch) {
  ASSERT_ND(batch_size <= kBatchMax);
  ArrayPage* pages[kBatchMax];
  LookupRoute routes[kBatchMax];
  uint16_t index_batch[kBatchMax];
  ArrayPage* root_page;
  CHECK_ERROR_CODE(get_root_page(context, true, &root_page));
  ASSERT_ND(!root_page->header().snapshot_);
  uint16_t levels = get_levels();
  const uint16_t payload_size = get_payload_size();

  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(offset_batch[i] < get_array_size());
    routes[i] = control_block_->route_finder_.find_route(offset_batch[i]);
    if (levels <= 1U) {
      assorted::prefetch_cacheline(root_page->get_leaf_record(
        routes[i].route[0],
        payload_size));
    } else {
      assorted::prefetch_cacheline(&root_page->get_interior_record(routes[i].route[levels - 1]));
    }
    pages[i] = root_page;
    ASSERT_ND(pages[i]->get_array_range().contains(offset_batch[i]));
    index_batch[i] = routes[i].route[levels - 1];
  }

  for (uint8_t level = levels - 1; level > 0; --level) {
    // as noted above, in==out case.
    CHECK_ERROR_CODE(follow_pointers_for_write_batch(
      context,
      batch_size,
      pages,
      index_batch,
      pages));

    for (uint8_t i = 0; i < batch_size; ++i) {
      ASSERT_ND(!pages[i]->header().snapshot_);
      ASSERT_ND(pages[i]->get_storage_id() == get_id());
      ASSERT_ND(!construct_volatile_page_pointer(pages[i]->header().page_id_).is_null());
      if (level == 1U) {
        assorted::prefetch_cacheline(pages[i]->get_leaf_record(
          routes[i].route[0],
          payload_size));
      } else {
        assorted::prefetch_cacheline(
          &pages[i]->get_interior_record(routes[i].route[levels - 1]));
      }
      index_batch[i] = routes[i].route[level - 1];
    }
  }

  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(pages[i]->is_leaf());
    ASSERT_ND(pages[i]->get_array_range().contains(offset_batch[i]));
    ASSERT_ND(pages[i]->get_array_range().begin_ + routes[i].route[0] == offset_batch[i]);
    ASSERT_ND(index_batch[i] == routes[i].route[0]);
    record_batch[i] = pages[i]->get_leaf_record(routes[i].route[0], payload_size);
  }
  return kErrorCodeOk;
}

#define CHECK_AND_ASSERT(x) do { ASSERT_ND(x); if (!(x)) \
  return ERROR_STACK(kErrorCodeStrArrayFailedVerification); } while (0)

ErrorStack ArrayStoragePimpl::verify_single_thread(thread::Thread* context) {
  ArrayPage* root;
  WRAP_ERROR_CODE(get_root_page(context, false, &root));
  return verify_single_thread(context, root);
}
ErrorStack ArrayStoragePimpl::verify_single_thread(thread::Thread* context, ArrayPage* page) {
  const memory::GlobalVolatilePageResolver& resolver = context->get_global_volatile_page_resolver();
  if (page->is_leaf()) {
    for (uint16_t i = 0; i < page->get_leaf_record_count(); ++i) {
      Record* record = page->get_leaf_record(i, get_payload_size());
      CHECK_AND_ASSERT(!record->owner_id_.is_being_written());
      CHECK_AND_ASSERT(!record->owner_id_.is_deleted());
      CHECK_AND_ASSERT(!record->owner_id_.is_keylocked());
      CHECK_AND_ASSERT(!record->owner_id_.is_moved());
      CHECK_AND_ASSERT(record->owner_id_.lock_.get_tail_waiter() == 0);
      CHECK_AND_ASSERT(record->owner_id_.lock_.get_tail_waiter_block() == 0);
    }
  } else {
    for (uint16_t i = 0; i < kInteriorFanout; ++i) {
      DualPagePointer &child_pointer = page->get_interior_record(i);
      VolatilePagePointer page_id = child_pointer.volatile_pointer_;
      if (page_id.components.offset != 0) {
        // then recurse
        ArrayPage* child_page = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(page_id));
        CHECK_ERROR(verify_single_thread(context, child_page));
      }
    }
  }
  return kRetOk;
}

ErrorCode ArrayStoragePimpl::follow_pointers_for_read_batch(
  thread::Thread* context,
  uint16_t batch_size,
  bool* in_snapshot,
  ArrayPage** parents,
  const uint16_t* index_in_parents,
  ArrayPage** out) {
  DualPagePointer* pointers[kBatchMax];
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(!parents[i]->is_leaf());
    ASSERT_ND(in_snapshot[i] == parents[i]->header().snapshot_);
    pointers[i] = &parents[i]->get_interior_record(index_in_parents[i]);
  }

#ifndef NDEBUG
  // there is a case of out==parents. for the assertion below, let's copy
  ArrayPage* parents_copy[kBatchMax];
  for (uint8_t i = 0; i < batch_size; ++i) {
    parents_copy[i] = parents[i];
  }
#endif  // NDEBUG

  CHECK_ERROR_CODE(context->follow_page_pointers_for_read_batch(
    batch_size,
    array_volatile_page_init,
    false,
    true,
    pointers,
    reinterpret_cast<Page**>(parents),
    index_in_parents,
    in_snapshot,
    reinterpret_cast<Page**>(out)));

#ifndef NDEBUG
  for (uint8_t i = 0; i < batch_size; ++i) {
    ArrayPage* page = out[i];
    ASSERT_ND(page != nullptr);
    /*
    if ((uintptr_t)(page) == 0xdadadadadadadadaULL) {
      for (uint8_t j = 0; j < batch_size; ++j) {
        in_snapshot[j] = parents_copy[j]->header().snapshot_;
      }
      CHECK_ERROR_CODE(context->follow_page_pointers_for_read_batch(
        batch_size,
        array_volatile_page_init,
        false,
        true,
        pointers,
        reinterpret_cast<Page**>(parents_copy),
        index_in_parents,
        in_snapshot,
        reinterpret_cast<Page**>(out)));
    }
    */
    ASSERT_ND(in_snapshot[i] == page->header().snapshot_);
    ASSERT_ND(page->get_level() + 1U == parents_copy[i]->get_level());
    if (page->is_leaf()) {
      xct::XctId xct_id = page->get_leaf_record(0, get_payload_size())->owner_id_.xct_id_;
      ASSERT_ND(xct_id.is_valid());
    }
  }
#endif  // NDEBUG
  return kErrorCodeOk;
}

ErrorCode ArrayStoragePimpl::follow_pointers_for_write_batch(
  thread::Thread* context,
  uint16_t batch_size,
  ArrayPage** parents,
  const uint16_t* index_in_parents,
  ArrayPage** out) {
  DualPagePointer* pointers[kBatchMax];
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(!parents[i]->is_leaf());
    ASSERT_ND(!parents[i]->header().snapshot_);
    pointers[i] = &parents[i]->get_interior_record(index_in_parents[i]);
  }

#ifndef NDEBUG
  // there is a case of out==parents. for the assertion below, let's copy
  ArrayPage* parents_copy[kBatchMax];
  for (uint8_t i = 0; i < batch_size; ++i) {
    parents_copy[i] = parents[i];
  }
#endif  // NDEBUG

  CHECK_ERROR_CODE(context->follow_page_pointers_for_write_batch(
    batch_size,
    array_volatile_page_init,
    pointers,
    reinterpret_cast<Page**>(parents),
    index_in_parents,
    reinterpret_cast<Page**>(out)));

#ifndef NDEBUG
  for (uint8_t i = 0; i < batch_size; ++i) {
    ArrayPage* page = out[i];
    ASSERT_ND(page != nullptr);
    ASSERT_ND(!page->header().snapshot_);
    ASSERT_ND(page->get_level() + 1U == parents_copy[i]->get_level());
    if (page->is_leaf()) {
      xct::XctId xct_id = page->get_leaf_record(0, get_payload_size())->owner_id_.xct_id_;
      ASSERT_ND(xct_id.is_valid());
    }
  }
#endif  // NDEBUG
  return kErrorCodeOk;
}

// Explicit instantiations for each type
// @cond DOXYGEN_IGNORE
#define EXPLICIT_INSTANTIATION_GET(x) template ErrorCode ArrayStorage::get_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x *payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET);

#define EX_GET_BATCH(x) template ErrorCode ArrayStorage::get_record_primitive_batch< x > \
  (thread::Thread* context, uint16_t payload_offset, \
  uint16_t batch_size, const ArrayOffset* offset_batch, x *payload)
INSTANTIATE_ALL_NUMERIC_TYPES(EX_GET_BATCH);

#define EX_GET_BATCH_IMPL(x) template ErrorCode \
  ArrayStoragePimpl::get_record_primitive_batch< x > \
  (thread::Thread* context, uint16_t payload_offset, \
  uint16_t batch_size, const ArrayOffset* offset_batch, x *payload)
INSTANTIATE_ALL_NUMERIC_TYPES(EX_GET_BATCH_IMPL);

#define EXPLICIT_INSTANTIATION_OV(x) template ErrorCode\
  ArrayStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_OV);

#define EX_OV_REC(x) template ErrorCode\
  ArrayStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, Record* record, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EX_OV_REC);

#define EX_OV_REC_IMPL(x) template ErrorCode\
  ArrayStoragePimpl::overwrite_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, Record* record, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EX_OV_REC_IMPL);

#define EXPLICIT_INSTANTIATION_INC(x) template ErrorCode ArrayStorage::increment_record< x > \
  (thread::Thread* context, ArrayOffset offset, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_INC);

#define EXPLICIT_INSTANTIATION_GET_IMPL(x) template ErrorCode \
  ArrayStoragePimpl::get_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x *payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_IMPL);

#define EXPLICIT_INSTANTIATION_GET_OV_IMPL(x) template ErrorCode \
  ArrayStoragePimpl::overwrite_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_OV_IMPL);

#define EXPLICIT_INSTANTIATION_GET_INC_IMPL(x) template ErrorCode \
  ArrayStoragePimpl::increment_record< x > \
  (thread::Thread* context, ArrayOffset offset, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_INC_IMPL);

#define EX_INC1S(x) template ErrorCode ArrayStorage::increment_record_oneshot< x > \
  (thread::Thread* context, ArrayOffset offset, x value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EX_INC1S);

#define EX_INC1S_IMPL(x) template ErrorCode ArrayStoragePimpl::increment_record_oneshot< x > \
  (thread::Thread* context, ArrayOffset offset, x value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EX_INC1S_IMPL);
// @endcond

}  // namespace array
}  // namespace storage
}  // namespace foedus
