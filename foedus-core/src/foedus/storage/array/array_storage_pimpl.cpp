/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
#include "foedus/log/thread_log_buffer_impl.hpp"
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

ArrayPage* ArrayStoragePimpl::get_root_page() {
  // Array storage is guaranteed to keep the root page as a volatile page.
  return reinterpret_cast<ArrayPage*>(
    engine_->get_memory_manager()->get_global_volatile_page_resolver().resolve_offset(
    control_block_->root_page_pointer_.volatile_pointer_));
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
  memory::PageReleaseBatch release_batch(engine_);
  ArrayStoragePimpl::release_pages_recursive(
    engine_->get_memory_manager()->get_global_volatile_page_resolver(),
    &release_batch,
    control_block_->root_page_pointer_.volatile_pointer_);
  release_batch.release_all();
  control_block_->root_page_pointer_.volatile_pointer_.word = 0;
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
  control_block_->levels_ = levels;
  control_block_->route_finder_ = LookupRouteFinder(levels, payload_size);
  control_block_->root_page_pointer_.snapshot_pointer_ = 0;
  control_block_->root_page_pointer_.volatile_pointer_.word = 0;
  control_block_->meta_.root_snapshot_page_id_ = 0;

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
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    get_id(),
    &record->owner_id_,
    snapshot_record,
    [record, payload, payload_offset, payload_count](xct::XctId /*observed*/){
      std::memcpy(payload, record->payload_ + payload_offset, payload_count);
      return kErrorCodeOk;
    }));
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
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    get_id(),
    &record->owner_id_,
    snapshot_record,
    [record, payload, payload_offset](xct::XctId /*observed*/){
      char* ptr = record->payload_ + payload_offset;
      *payload = *reinterpret_cast<const T*>(ptr);
      return kErrorCodeOk;
    }));
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
    current_xct.get_isolation_level() != xct::kDirtyReadPreferSnapshot &&
    current_xct.get_isolation_level() != xct::kDirtyReadPreferVolatile) {
    xct::XctId observed(record->owner_id_.xct_id_);
    assorted::memory_fence_consume();
    CHECK_ERROR_CODE(current_xct.add_to_read_set(get_id(), observed, &record->owner_id_));
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
  if (current_xct.get_isolation_level() != xct::kDirtyReadPreferSnapshot &&
    current_xct.get_isolation_level() != xct::kDirtyReadPreferVolatile) {
    xct::XctId observed((*record)->owner_id_.xct_id_);
    assorted::memory_fence_consume();
    CHECK_ERROR_CODE(current_xct.add_to_read_set(get_id(), observed, &((*record)->owner_id_)));
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
  T tmp;
  T* tmp_address = &tmp;
  // NOTE if we directly pass value and increment there, we might do it multiple times!
  // optimistic_read_protocol() retries if there are version mismatch.
  // so it must be idempotent. be careful!
  // TODO(Hideaki) Only Array's increment can be the rare "write-set only" log.
  // other increments have to check deletion bit at least.
  // to make use of it, we should have array increment log with primitive type as parameter.
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    get_id(),
    &record->owner_id_,
    false,
    [record, tmp_address, payload_offset](xct::XctId /*observed*/){
      char* ptr = record->payload_ + payload_offset;
      *tmp_address = *reinterpret_cast<const T*>(ptr);
      return kErrorCodeOk;
    }));
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
  ArrayPage* current_page = get_root_page();
  uint16_t levels = get_levels();
  ASSERT_ND(current_page->get_array_range().contains(offset));
  LookupRoute route = control_block_->route_finder_.find_route(offset);
  bool in_snapshot = false;
  for (uint8_t level = levels - 1; level > 0; --level) {
    ASSERT_ND(current_page->get_array_range().contains(offset));
    DualPagePointer& pointer = current_page->get_interior_record(route.route[level]);
    CHECK_ERROR_CODE(follow_pointer(
      context,
      route,
      level,
      in_snapshot,
      false,
      &pointer,
      &current_page));
    in_snapshot = current_page->header().snapshot_;
  }
  ASSERT_ND(current_page->is_leaf());
  ASSERT_ND(current_page->get_array_range().contains(offset));
  ASSERT_ND(current_page->get_array_range().begin_ + route.route[0] == offset);
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
  ArrayPage* current_page = get_root_page();
  uint16_t levels = get_levels();
  ASSERT_ND(current_page->get_array_range().contains(offset));
  LookupRoute route = control_block_->route_finder_.find_route(offset);
  for (uint8_t level = levels - 1; level > 0; --level) {
    ASSERT_ND(current_page->get_array_range().contains(offset));
    CHECK_ERROR_CODE(follow_pointer(
      context,
      route,
      level,
      false,
      true,
      &current_page->get_interior_record(route.route[level]),
      &current_page));
  }
  ASSERT_ND(current_page->is_leaf());
  ASSERT_ND(current_page->get_array_range().contains(offset));
  ASSERT_ND(current_page->get_array_range().begin_ + route.route[0] == offset);
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
  if (current_xct.get_isolation_level() != xct::kDirtyReadPreferSnapshot &&
      current_xct.get_isolation_level() != xct::kDirtyReadPreferVolatile) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      if (!snapshot_record_batch[i]) {
        xct::XctId observed(record_batch[i]->owner_id_.xct_id_);
        CHECK_ERROR_CODE(current_xct.add_to_read_set(
          get_id(),
          observed,
          &record_batch[i]->owner_id_));
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
  if (current_xct.get_isolation_level() != xct::kDirtyReadPreferSnapshot &&
      current_xct.get_isolation_level() != xct::kDirtyReadPreferVolatile) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      if (!snapshot_record_batch[i]) {
        xct::XctId observed(record_batch[i]->owner_id_.xct_id_);
        CHECK_ERROR_CODE(current_xct.add_to_read_set(
          get_id(),
          observed,
          &record_batch[i]->owner_id_));
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
  if (current_xct.get_isolation_level() != xct::kDirtyReadPreferSnapshot &&
      current_xct.get_isolation_level() != xct::kDirtyReadPreferVolatile) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      xct::XctId observed(record_batch[i]->owner_id_.xct_id_);
      CHECK_ERROR_CODE(current_xct.add_to_read_set(
        get_id(),
        observed,
        &record_batch[i]->owner_id_));
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
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(page_batch[i]);
    ASSERT_ND(page_batch[i]->is_leaf());
    ASSERT_ND(page_batch[i]->get_array_range().contains(offset_batch[i]));
    out_batch[i] = page_batch[i]->get_leaf_record(index_batch[i], get_payload_size());
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
  ArrayPage* root_page = get_root_page();
  uint16_t levels = get_levels();
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(offset_batch[i] < get_array_size());
    routes[i] = control_block_->route_finder_.find_route(offset_batch[i]);
    if (levels == 0) {
      assorted::prefetch_cacheline(root_page->get_leaf_record(
        routes[i].route[0],
        get_payload_size()));
    } else {
      assorted::prefetch_cacheline(&root_page->get_interior_record(routes[i].route[levels - 1]));
    }
    out_batch[i] = root_page;
    snapshot_page_batch[i] = false;
  }

  for (uint8_t level = levels - 1; level > 0; --level) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      ASSERT_ND(out_batch[i]->get_array_range().contains(offset_batch[i]));
      DualPagePointer& pointer = out_batch[i]->get_interior_record(routes[i].route[level]);
      CHECK_ERROR_CODE(follow_pointer(
        context,
        routes[i],
        level,
        snapshot_page_batch[i],
        false,
        &pointer,
        &(out_batch[i])));
      snapshot_page_batch[i] = out_batch[i]->header().snapshot_;
      if (level == 1U) {
        assorted::prefetch_cacheline(out_batch[i]->get_leaf_record(
          routes[i].route[0],
          get_payload_size()));
      } else {
        assorted::prefetch_cacheline(
          &out_batch[i]->get_interior_record(routes[i].route[levels - 1]));
      }
    }
  }
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(out_batch[i]->is_leaf());
    ASSERT_ND(out_batch[i]->get_array_range().contains(offset_batch[i]));
    ASSERT_ND(out_batch[i]->get_array_range().begin_ + routes[i].route[0] == offset_batch[i]);
    index_batch[i] = routes[i].route[0];
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
  ArrayPage* root_page = get_root_page();
  uint16_t levels = get_levels();
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(offset_batch[i] < get_array_size());
    routes[i] = control_block_->route_finder_.find_route(offset_batch[i]);
    if (levels == 0) {
      assorted::prefetch_cacheline(
        root_page->get_leaf_record(
        routes[i].route[0],
        get_payload_size()));
    } else {
      assorted::prefetch_cacheline(
        &root_page->get_interior_record(routes[i].route[levels - 1]));
    }
    pages[i] = root_page;
  }

  for (uint8_t level = levels - 1; level > 0; --level) {
    for (uint8_t i = 0; i < batch_size; ++i) {
      ASSERT_ND(pages[i]->get_array_range().contains(offset_batch[i]));
      CHECK_ERROR_CODE(follow_pointer(
        context,
        routes[i],
        level,
        false,
        true,
        &pages[i]->get_interior_record(routes[i].route[level]),
        &pages[i]));
      if (level == 1U) {
        assorted::prefetch_cacheline(pages[i]->get_leaf_record(
          routes[i].route[0],
          get_payload_size()));
      } else {
        assorted::prefetch_cacheline(
          &pages[i]->get_interior_record(routes[i].route[levels - 1]));
      }
    }
  }
  for (uint8_t i = 0; i < batch_size; ++i) {
    ASSERT_ND(pages[i]->is_leaf());
    ASSERT_ND(pages[i]->get_array_range().contains(offset_batch[i]));
    ASSERT_ND(pages[i]->get_array_range().begin_ + routes[i].route[0] == offset_batch[i]);
    record_batch[i] = pages[i]->get_leaf_record(routes[i].route[0], get_payload_size());
  }
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::follow_pointer(
  thread::Thread* context,
  LookupRoute route,
  uint8_t parent_level,
  bool in_snapshot,
  bool for_write,
  DualPagePointer* pointer,
  ArrayPage** out) {
  ASSERT_ND(!in_snapshot || !for_write);  // if we are modifying, we must be in volatile world
  ASSERT_ND(parent_level > 0);
  ArrayVolatileInitializer initializer(
    control_block_->meta_,
    parent_level - 1U,
    control_block_->levels_,
    context->get_current_xct().get_id().get_epoch(),
    route);
  return context->follow_page_pointer(
    &initializer,  // array storage might have null pointer. in that case create an empty new page
    false,  // if both volatile/snapshot null, create a new volatile (logically all-zero)
    for_write,
    !in_snapshot,  // if we are already in snapshot world, no need to take more pointer set
    false,
    pointer,
    reinterpret_cast<Page**>(out));
}

#define CHECK_AND_ASSERT(x) do { ASSERT_ND(x); if (!(x)) \
  return ERROR_STACK(kErrorCodeStrArrayFailedVerification); } while (0)

ErrorStack ArrayStoragePimpl::verify_single_thread(thread::Thread* context) {
  return verify_single_thread(context, get_root_page());
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
      CHECK_AND_ASSERT(record->owner_id_.lock_.get_version() == 0);
      CHECK_AND_ASSERT(record->owner_id_.lock_.get_key_lock()->get_tail_waiter() == 0);
      CHECK_AND_ASSERT(record->owner_id_.lock_.get_key_lock()->get_tail_waiter_block() == 0);
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

/////////////////////////////////////////////////////////////////////////////
///
///  Composer related methods
///
/////////////////////////////////////////////////////////////////////////////
ArrayPage* ArrayStoragePimpl::resolve_volatile(VolatilePagePointer pointer) {
  if (pointer.is_null()) {
    return nullptr;
  }
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  return reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(pointer));
}

ErrorStack ArrayStoragePimpl::replace_pointers(const Composer::ReplacePointersArguments& args) {
  // First, install the root page snapshot pointer.
  control_block_->meta_.root_snapshot_page_id_ = args.new_root_page_pointer_;
  control_block_->root_page_pointer_.snapshot_pointer_ = args.new_root_page_pointer_;
  ++(*args.installed_count_);

  if (get_meta().keeps_all_volatile_pages()) {
    LOG(INFO) << "Keep-all-volatile: Storage-" << control_block_->meta_.name_
      << " is configured to keep all volatile pages.";
    // in this case, there isn't much point to install snapshot pages to volatile images.
    // they will not be used anyways.
    return kRetOk;
  }

  // Second, we iterate through all existing volatile pages to 1) install snapshot pages
  // and 2) drop volatile pages of level-3 or deeper (if the storage has only 2 levels, keeps all).
  // this "level-3 or deeper" is a configuration per storage.
  // Even if the volatile page is deeper than that, we keep them if it contains newer modification,
  // including descendants (so, probably we will keep higher levels anyways).
  VolatilePagePointer root_volatile_pointer = control_block_->root_page_pointer_.volatile_pointer_;
  ArrayPage* volatile_page = resolve_volatile(root_volatile_pointer);
  if (volatile_page == nullptr) {
    LOG(WARNING) << "Mmm? no volatile root page?? then why included in this snapshot..";
    return kRetOk;
  }

  if (volatile_page->is_leaf()) {
    replace_pointers_leaf(args, &control_block_->root_page_pointer_, volatile_page);
  } else {
    bool kept_volatile;
    CHECK_ERROR(replace_pointers_recurse(
      args,
      &control_block_->root_page_pointer_,
      &kept_volatile,
      volatile_page));
  }
  return kRetOk;
}

ErrorStack ArrayStoragePimpl::replace_pointers_recurse(
  const Composer::ReplacePointersArguments& args,
  DualPagePointer* pointer,
  bool* kept_volatile,
  ArrayPage* volatile_page) {
  ASSERT_ND(!volatile_page->header().snapshot_);
  ASSERT_ND(!volatile_page->is_leaf());
  *kept_volatile = false;

  // Explore/replace children first because we need to know if there is new modification
  // in that case, we must keep this volatile page, too.
  ASSERT_ND(!volatile_page->is_leaf());
  if (is_to_keep_volatile(volatile_page->get_level())) {
    *kept_volatile = true;
  }
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    DualPagePointer &child_pointer = volatile_page->get_interior_record(i);
    if (!child_pointer.volatile_pointer_.is_null()) {
      ArrayPage* child_page = resolve_volatile(child_pointer.volatile_pointer_);
      bool child_kept = false;
      if (child_page->is_leaf()) {
        child_kept = replace_pointers_leaf(args, &child_pointer, child_page);
      } else {
        CHECK_ERROR(replace_pointers_recurse(args, &child_pointer, &child_kept, child_page));
      }

      if (child_kept) {
        *kept_volatile = true;
      }
    }
  }

  if (!*kept_volatile) {
    // We just drop this volatile page. done.
    args.drop_volatile_page(pointer->volatile_pointer_);
    pointer->volatile_pointer_.components.offset = 0;
    return kRetOk;
  }

  // Then we have to keep this volatile page.
  // if the snapshot pointer points to a newly created page, we have to reflect install
  // new snapshot pointers. So, read the snapshot page.
  snapshot::SnapshotId snapshot_id
    = extract_snapshot_id_from_snapshot_pointer(pointer->snapshot_pointer_);
  if (snapshot_id != args.snapshot_.id_) {
    DVLOG(1) << "This is not part of this snapshot, so no new pointers to install.";
    return kRetOk;
  }

  DVLOG(1) << "The new snapshot covers this page. Let's install new snapshot pointers";
  ASSERT_ND(args.work_memory_->get_size() >= sizeof(ArrayPage));
  ArrayPage* snapshot_page = reinterpret_cast<ArrayPage*>(args.work_memory_->get_block());
  WRAP_ERROR_CODE(args.read_snapshot_page(pointer->snapshot_pointer_, snapshot_page));
  ASSERT_ND(volatile_page->get_level() == snapshot_page->get_level());
  ASSERT_ND(volatile_page->get_array_range() == snapshot_page->get_array_range());
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    DualPagePointer &child_pointer = volatile_page->get_interior_record(i);
    SnapshotPagePointer new_pointer = snapshot_page->get_interior_record(i).snapshot_pointer_;
    if (new_pointer != child_pointer.snapshot_pointer_) {
      ASSERT_ND(new_pointer != 0);  // no transition from have-snapshot-page to no-snapshot-page
      child_pointer.snapshot_pointer_ = new_pointer;
      ++(*args.installed_count_);
    }
  }
  // Now we no longer need snapshot_page. This is why we need only one-page of work memory
  return kRetOk;
}

bool ArrayStoragePimpl::replace_pointers_leaf(
  const Composer::ReplacePointersArguments& args,
  DualPagePointer* pointer,
  ArrayPage* volatile_page) {
  ASSERT_ND(!volatile_page->header().snapshot_);
  ASSERT_ND(volatile_page->is_leaf());
  if (is_to_keep_volatile(volatile_page->get_level())) {
    return true;
  }
  bool kept_volatile = false;
  for (uint16_t i = 0; i < volatile_page->get_leaf_record_count(); ++i) {
    Record* record = volatile_page->get_leaf_record(i, get_payload_size());
    Epoch epoch = record->owner_id_.xct_id_.get_epoch();
    if (epoch.is_valid() && epoch > args.snapshot_.valid_until_epoch_) {
      // new record exists! so we must keep this volatile page
      kept_volatile = true;
      break;
    }
  }
  if (!kept_volatile) {
    args.drop_volatile_page(pointer->volatile_pointer_);
    pointer->volatile_pointer_.components.offset = 0;
  } else {
    DVLOG(1) << "Couldn't drop a leaf volatile page that has a recent modification";
  }
  return kept_volatile;
}
bool ArrayStoragePimpl::is_to_keep_volatile(uint16_t level) {
  uint16_t threshold = get_snapshot_drop_volatile_pages_threshold();
  uint16_t array_levels = get_levels();
  ASSERT_ND(level < array_levels);
  // examples:
  // when threshold=0, all levels (0~array_levels-1) should return false.
  // when threshold=1, only root level (array_levels-1) should return true
  // when threshold=2, upto array_levels-2..
  return threshold >= array_levels - level;
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
