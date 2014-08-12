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
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/page_pool.hpp"
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
bool        ArrayStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        ArrayStorage::exists()           const  { return pimpl_->exist_; }
uint16_t    ArrayStorage::get_payload_size() const  { return pimpl_->metadata_.payload_size_; }
ArrayOffset ArrayStorage::get_array_size()   const  { return pimpl_->metadata_.array_size_; }
uint8_t     ArrayStorage::get_levels()       const  { return pimpl_->levels_; }
StorageId   ArrayStorage::get_id()           const  { return pimpl_->metadata_.id_; }
const std::string& ArrayStorage::get_name()  const  { return pimpl_->metadata_.name_; }
const Metadata* ArrayStorage::get_metadata() const  { return &pimpl_->metadata_; }
const ArrayMetadata* ArrayStorage::get_array_metadata() const  { return &pimpl_->metadata_; }

ErrorCode ArrayStorage::get_record(
  thread::Thread* context, ArrayOffset offset, void *payload) {
  return get_record(context, offset, payload, 0, pimpl_->metadata_.payload_size_);
}

ErrorCode ArrayStorage::get_record(thread::Thread* context, ArrayOffset offset,
          void *payload, uint16_t payload_offset, uint16_t payload_count) {
  return pimpl_->get_record(context, offset, payload, payload_offset, payload_count);
}

template <typename T>
ErrorCode ArrayStorage::get_record_primitive(thread::Thread* context, ArrayOffset offset,
          T *payload, uint16_t payload_offset) {
  return pimpl_->get_record_primitive<T>(context, offset, payload, payload_offset);
}

ErrorCode ArrayStorage::overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) {
  return pimpl_->overwrite_record(context, offset, payload, payload_offset, payload_count);
}

template <typename T>
ErrorCode ArrayStorage::overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
          T payload, uint16_t payload_offset) {
  return pimpl_->overwrite_record_primitive<T>(context, offset, payload, payload_offset);
}

template <typename T>
ErrorCode ArrayStorage::increment_record(thread::Thread* context, ArrayOffset offset,
          T* value, uint16_t payload_offset) {
  return pimpl_->increment_record<T>(context, offset, value, payload_offset);
}

template <typename T>
ErrorCode ArrayStorage::increment_record_oneshot(
  thread::Thread* context,
  ArrayOffset offset,
  T value,
  uint16_t payload_offset) {
  return pimpl_->increment_record_oneshot<T>(context, offset, value, payload_offset);
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

ArrayStoragePimpl::ArrayStoragePimpl(Engine* engine, ArrayStorage* holder,
                   const ArrayMetadata &metadata, bool create)
  : engine_(engine),
  holder_(holder),
  metadata_(metadata),
  root_page_(nullptr),
  exist_(!create),
  levels_(calculate_levels(metadata)),
  route_finder_(levels_, metadata.payload_size_) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
  root_page_pointer_.snapshot_pointer_ = metadata.root_snapshot_page_id_;
  root_page_pointer_.volatile_pointer_.word = 0;
}

ErrorStack ArrayStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an array-storage " << *holder_ << " exists=" << exist_;
  std::vector<uint64_t> pages = calculate_required_pages(
    metadata_.array_size_, metadata_.payload_size_);

  if (exist_) {
    // initialize root_page_
  }
  return kRetOk;
}

void ArrayStoragePimpl::release_pages_recursive(
  memory::PageReleaseBatch* batch, ArrayPage* page, VolatilePagePointer volatile_page_id) {
  ASSERT_ND(volatile_page_id.components.offset != 0);
  if (!page->is_leaf()) {
    const memory::GlobalVolatilePageResolver& page_resolver
      = engine_->get_memory_manager().get_global_volatile_page_resolver();
    for (uint16_t i = 0; i < kInteriorFanout; ++i) {
      DualPagePointer &child_pointer = page->get_interior_record(i);
      VolatilePagePointer child_page_id = child_pointer.volatile_pointer_;
      if (child_page_id.components.offset != 0) {
        // then recurse
        ArrayPage* child_page = reinterpret_cast<ArrayPage*>(
          page_resolver.resolve_offset(child_page_id));
        release_pages_recursive(batch, child_page, child_page_id);
        child_pointer.volatile_pointer_.word = 0;
      }
    }
  }
  batch->release(volatile_page_id);
}

ErrorStack ArrayStoragePimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing an array-storage " << *holder_;
  if (root_page_) {
    LOG(INFO) << "Releasing all in-memory pages...";
    memory::PageReleaseBatch release_batch(engine_);
    release_pages_recursive(&release_batch, root_page_, root_page_pointer_.volatile_pointer_);
    release_batch.release_all();
    root_page_ = nullptr;
    root_page_pointer_.volatile_pointer_.word = 0;
  }
  return kRetOk;
}


ErrorStack ArrayStoragePimpl::create(thread::Thread* context) {
  if (exist_) {
    LOG(ERROR) << "This array-storage already exists: " << *holder_;
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  const uint16_t payload_size_aligned = (assorted::align8(metadata_.payload_size_));

  // Number of pages in each level. index=level.
  std::vector<uint64_t> pages = calculate_required_pages(
    metadata_.array_size_, metadata_.payload_size_);

  // The offset interval a single page represents in each level. index=level.
  // So, offset_intervals[0] is the number of records in a leaf page.
  std::vector<uint64_t> offset_intervals;
  offset_intervals.push_back(kDataSize / (payload_size_aligned + kRecordOverhead));
  for (uint8_t level = 1; level < levels_; ++level) {
    offset_intervals.push_back(offset_intervals[level - 1] * kInteriorFanout);
  }
  for (uint8_t level = 0; level < levels_; ++level) {
    LOG(INFO) << "Level-" << static_cast<int>(level) << " pages=" << pages[level]
      << " interval=" << offset_intervals[level];
  }

  Epoch initial_epoch = engine_->get_xct_manager().get_current_global_epoch();
  LOG(INFO) << "Newly creating an array-storage "  << *holder_ << " as epoch=" << initial_epoch;

  // TODO(Hideaki) This part must handle the case where RAM < Array Size
  // So far, we just crash in DivvyupPageGrabBatch::grab().

  // we create from left, keeping cursors on each level.
  // first, create the left-most in each level
  // All of the following, index=level
  std::vector<ArrayPage*> current_pages;
  std::vector<VolatilePagePointer> current_pages_ids;
  std::vector<uint16_t> current_records;
  // we grab free page from each node evenly.
  const memory::GlobalVolatilePageResolver& page_resolver
    = context->get_global_volatile_page_resolver();
  memory::DivvyupPageGrabBatch grab_batch(engine_);
  for (uint8_t level = 0; level < levels_; ++level) {
    VolatilePagePointer page_pointer = grab_batch.grab_evenly(0, pages[level]);
    ASSERT_ND(page_pointer.components.offset != 0);
    ArrayPage* page = reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(page_pointer));
    current_pages.push_back(page);
    current_pages_ids.push_back(page_pointer);
  }
  for (uint8_t level = 0; level < levels_; ++level) {
    ArrayPage* page = current_pages[level];
    ArrayRange range(0, offset_intervals[level]);
    if (range.end_ > metadata_.array_size_) {
      ASSERT_ND(level == levels_ - 1);
      range.end_ = metadata_.array_size_;
    }
    bool root = (level == levels_ - 1);
    page->initialize_volatile_page(
      initial_epoch,
      metadata_.id_,
      current_pages_ids[level],
      metadata_.payload_size_,
      level,
      root,
      range);

    if (level == 0) {
      current_records.push_back(0);
    } else {
      current_records.push_back(1);
      DualPagePointer& child_pointer = page->get_interior_record(0);
      child_pointer.snapshot_pointer_ = 0;
      child_pointer.volatile_pointer_ = current_pages_ids[level - 1];
      child_pointer.volatile_pointer_.components.flags = 0;
      child_pointer.volatile_pointer_.components.mod_count = 0;
    }
  }
  ASSERT_ND(current_pages.size() == levels_);
  ASSERT_ND(current_pages_ids.size() == levels_);
  ASSERT_ND(current_records.size() == levels_);

  // then moves on to right
  for (uint64_t leaf = 1; leaf < pages[0]; ++leaf) {
    VolatilePagePointer page_pointer = grab_batch.grab_evenly(leaf, pages[0]);
    ASSERT_ND(page_pointer.components.offset != 0);
    ArrayPage* page = reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(page_pointer));

    ArrayRange range(current_pages[0]->get_array_range().end_,
             current_pages[0]->get_array_range().end_ + offset_intervals[0]);
    if (range.end_ > metadata_.array_size_) {
      range.end_ = metadata_.array_size_;
    }
    page->initialize_volatile_page(
      initial_epoch,
      metadata_.id_,
      page_pointer,
      metadata_.payload_size_,
      0,
      false,
      range);
    current_pages[0] = page;
    current_pages_ids[0] = page_pointer;
    // current_records[0] is always 0

    // push it up to parent, potentially up to root
    for (uint8_t level = 1; level < levels_; ++level) {
      if (current_records[level] == kInteriorFanout) {
        VLOG(2) << "leaf=" << leaf << ", interior level=" << static_cast<int>(level);
        // On same NUMA node as the leaf.
        VolatilePagePointer interior_pointer = grab_batch.grab(page_pointer.components.numa_node);
        ASSERT_ND(interior_pointer.components.offset != 0);
        ArrayPage* interior_page = reinterpret_cast<ArrayPage*>(
          page_resolver.resolve_offset(interior_pointer));
        ArrayRange interior_range(current_pages[level]->get_array_range().end_,
             current_pages[level]->get_array_range().end_ + offset_intervals[level]);
        if (range.end_ > metadata_.array_size_) {
          range.end_ = metadata_.array_size_;
        }
        bool root = (level == levels_ - 1);
        interior_page->initialize_volatile_page(
          initial_epoch,
          metadata_.id_,
          interior_pointer,
          metadata_.payload_size_,
          level,
          root,
          interior_range);

        DualPagePointer& child_pointer = interior_page->get_interior_record(0);
        child_pointer.snapshot_pointer_ = 0;
        child_pointer.volatile_pointer_ = current_pages_ids[level - 1];
        child_pointer.volatile_pointer_.components.flags = 0;
        child_pointer.volatile_pointer_.components.mod_count = 0;
        current_pages[level] = interior_page;
        current_pages_ids[level] = interior_pointer;
        current_records[level] = 1;
        // also inserts this to parent
      } else {
        DualPagePointer& child_pointer = current_pages[level]->get_interior_record(
          current_records[level]);
        child_pointer.snapshot_pointer_ = 0;
        child_pointer.volatile_pointer_ = current_pages_ids[level - 1];
        child_pointer.volatile_pointer_.components.flags = 0;
        child_pointer.volatile_pointer_.components.mod_count = 0;
        ++current_records[level];
        break;
      }
    }
  }

  root_page_pointer_.snapshot_pointer_ = 0;
  root_page_pointer_.volatile_pointer_ = current_pages_ids[levels_ - 1];
  root_page_pointer_.volatile_pointer_.components.flags = 0;
  root_page_pointer_.volatile_pointer_.components.mod_count = 0;
  root_page_ = current_pages[levels_ - 1];
  LOG(INFO) << "Newly created an array-storage " << *holder_;
  exist_ = true;
  engine_->get_storage_manager().get_pimpl()->register_storage(holder_);
  return kRetOk;
}

inline ErrorCode ArrayStoragePimpl::locate_record_for_read(
  thread::Thread* context,
  ArrayOffset offset,
  Record** out,
  bool* snapshot_record) {
  ASSERT_ND(is_initialized());
  ASSERT_ND(offset < metadata_.array_size_);
  uint16_t index = 0;
  ArrayPage* page = nullptr;
  CHECK_ERROR_CODE(lookup_for_read(context, offset, &page, &index, snapshot_record));
  ASSERT_ND(page);
  ASSERT_ND(page->is_leaf());
  ASSERT_ND(page->get_array_range().contains(offset));
  *out = page->get_leaf_record(index);
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::locate_record_for_write(
  thread::Thread* context,
  ArrayOffset offset,
  Record** out) {
  ASSERT_ND(is_initialized());
  ASSERT_ND(offset < metadata_.array_size_);
  uint16_t index = 0;
  ArrayPage* page = nullptr;
  CHECK_ERROR_CODE(lookup_for_write(context, offset, &page, &index));
  ASSERT_ND(page);
  ASSERT_ND(page->is_leaf());
  ASSERT_ND(page->get_array_range().contains(offset));
  *out = page->get_leaf_record(index);
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::get_record(
  thread::Thread* context,
  ArrayOffset offset,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  ASSERT_ND(payload_offset + payload_count <= metadata_.payload_size_);
  Record *record = nullptr;
  bool snapshot_record;
  CHECK_ERROR_CODE(locate_record_for_read(context, offset, &record, &snapshot_record));
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    holder_,
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
  ASSERT_ND(payload_offset + sizeof(T) <= metadata_.payload_size_);
  Record *record = nullptr;
  bool snapshot_record;
  CHECK_ERROR_CODE(locate_record_for_read(context, offset, &record, &snapshot_record));
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    holder_,
    &record->owner_id_,
    snapshot_record,
    [record, payload, payload_offset](xct::XctId /*observed*/){
      char* ptr = record->payload_ + payload_offset;
      *payload = *reinterpret_cast<const T*>(ptr);
      return kErrorCodeOk;
    }));
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) {
  ASSERT_ND(payload_offset + payload_count <= metadata_.payload_size_);
  Record *record = nullptr;
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, &record));

  // write out log
  uint16_t log_length = ArrayOverwriteLogType::calculate_log_length(payload_count);
  ArrayOverwriteLogType* log_entry = reinterpret_cast<ArrayOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, offset, payload, payload_offset, payload_count);
  return context->get_current_xct().add_to_write_set(
    holder_,
    &record->owner_id_,
    record->payload_,
    log_entry);
}

template <typename T>
ErrorCode ArrayStoragePimpl::overwrite_record_primitive(
      thread::Thread* context, ArrayOffset offset, T payload, uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= metadata_.payload_size_);
  Record *record = nullptr;
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, &record));

  // write out log
  uint16_t log_length = ArrayOverwriteLogType::calculate_log_length(sizeof(T));
  ArrayOverwriteLogType* log_entry = reinterpret_cast<ArrayOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate_primitive<T>(metadata_.id_, offset, payload, payload_offset);
  return context->get_current_xct().add_to_write_set(
    holder_,
    &record->owner_id_,
    record->payload_,
    log_entry);
}

template <typename T>
ErrorCode ArrayStoragePimpl::increment_record(
      thread::Thread* context, ArrayOffset offset, T* value, uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= metadata_.payload_size_);
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
    holder_,
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
  log_entry->populate_primitive<T>(metadata_.id_, offset, *value, payload_offset);
  return context->get_current_xct().add_to_write_set(
    holder_,
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
  ASSERT_ND(payload_offset + sizeof(T) <= metadata_.payload_size_);
  Record *record = nullptr;
  CHECK_ERROR_CODE(locate_record_for_write(context, offset, &record));
  ValueType type = to_value_type<T>();
  uint16_t log_length = ArrayIncrementLogType::calculate_log_length(type);
  ArrayIncrementLogType* log_entry = reinterpret_cast<ArrayIncrementLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate<T>(metadata_.id_, offset, value, payload_offset);
  return context->get_current_xct().add_to_write_set(
    holder_,
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
  ASSERT_ND(is_initialized());
  ASSERT_ND(offset < metadata_.array_size_);
  ASSERT_ND(out);
  ASSERT_ND(index);
  ArrayPage* current_page = root_page_;
  ASSERT_ND(current_page->get_array_range().contains(offset));
  LookupRoute route = route_finder_.find_route(offset);
  const memory::GlobalVolatilePageResolver& page_resolver
    = context->get_global_volatile_page_resolver();
  xct::Xct& current_xct = context->get_current_xct();
  bool followed_snapshot_pointer = false;
  for (uint8_t level = levels_ - 1; level > 0; --level) {
    ASSERT_ND(current_page->get_array_range().contains(offset));
    DualPagePointer& pointer = current_page->get_interior_record(route.route[level]);
    storage::VolatilePagePointer volatile_pointer = pointer.volatile_pointer_;
    if (followed_snapshot_pointer) {
      // we already followed a snapshot pointer, so everything under it is stable.
      // just follow the snapstho pointer
      ASSERT_ND(pointer.snapshot_pointer_ != 0);
      ASSERT_ND(pointer.volatile_pointer_.word == 0);
      CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(
        pointer.snapshot_pointer_,
        reinterpret_cast<Page**>(&current_page)));
    } else if (volatile_pointer.components.offset == 0
      || current_xct.get_isolation_level() == xct::kDirtyReadPreferSnapshot) {
      // then read from snapshot page. this is the beginning point to follow a snapshot pointer,
      // so we have to take a pointer set in case someone else installs a new volatile pointer
      // (after here, everything is stable).
      ASSERT_ND(pointer.snapshot_pointer_ != 0);
      followed_snapshot_pointer = true;
      current_xct.add_to_pointer_set(&pointer.volatile_pointer_, volatile_pointer);
      CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(
        pointer.snapshot_pointer_,
        reinterpret_cast<Page**>(&current_page)));
    } else {
      // NOTE: In Array storage, we don't have to take a ptr set for following a volatile pointer
      // because we don't swap volatile pointer like Masstree's page split.
      // The only case we change volatile pointer is for snapshot thread to drop volatile pages
      // that are equivalent to snapshot pages, so it never affects serializability.
      current_page = reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(volatile_pointer));
    }
  }
  ASSERT_ND(current_page->is_leaf());
  ASSERT_ND(current_page->get_array_range().contains(offset));
  ASSERT_ND(current_page->get_array_range().begin_ + route.route[0] == offset);
  *out = current_page;
  *index = route.route[0];
  *snapshot_page = followed_snapshot_pointer;
  return kErrorCodeOk;
}

inline ErrorCode ArrayStoragePimpl::lookup_for_write(
  thread::Thread* context,
  ArrayOffset offset,
  ArrayPage** out,
  uint16_t* index) {
  ASSERT_ND(is_initialized());
  ASSERT_ND(offset < metadata_.array_size_);
  ASSERT_ND(out);
  ASSERT_ND(index);
  ArrayPage* current_page = root_page_;
  ASSERT_ND(current_page->get_array_range().contains(offset));
  LookupRoute route = route_finder_.find_route(offset);
  const memory::GlobalVolatilePageResolver& page_resolver
    = context->get_global_volatile_page_resolver();
  for (uint8_t level = levels_ - 1; level > 0; --level) {
    ASSERT_ND(current_page->get_array_range().contains(offset));
    DualPagePointer& pointer = current_page->get_interior_record(route.route[level]);
    storage::VolatilePagePointer volatile_pointer = pointer.volatile_pointer_;
    if (volatile_pointer.components.offset == 0) {
      ASSERT_ND(pointer.snapshot_pointer_ != 0);
      // then we have to install a new volatile page, starting from the snapshot page image.
      // this should, hopefully, do not happen too often. if the page is worth keeping as
      // a volatile page, snapshot thread shouldn't drop it or at least the page should
      // come back during the grace period.
      CHECK_ERROR_CODE(context->install_a_volatile_page(
        &pointer,
        reinterpret_cast<Page**>(&current_page)));
    } else {
      current_page = reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(volatile_pointer));
    }
  }
  ASSERT_ND(current_page->is_leaf());
  ASSERT_ND(current_page->get_array_range().contains(offset));
  ASSERT_ND(current_page->get_array_range().begin_ + route.route[0] == offset);
  *out = current_page;
  *index = route.route[0];
  return kErrorCodeOk;
}

// Explicit instantiations for each type
// @cond DOXYGEN_IGNORE
#define EXPLICIT_INSTANTIATION_GET(x) template ErrorCode ArrayStorage::get_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x *payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET);

#define EXPLICIT_INSTANTIATION_OV(x) template ErrorCode\
  ArrayStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_OV);

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
