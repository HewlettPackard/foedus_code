/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/record.hpp>
#include <foedus/storage/array/array_id.hpp>
#include <foedus/storage/array/array_log_types.hpp>
#include <foedus/storage/array/array_metadata.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_storage_pimpl.hpp>
#include <foedus/storage/array/array_page_impl.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/storage_manager_pimpl.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/page_pool.hpp>
#include <foedus/xct/xct.hpp>
#include <foedus/xct/xct_inl.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/engine.hpp>
#include <glog/logging.h>
#include <cinttypes>  // for std::lldiv
#include <cstdlib>  // for std::lldiv_t
#include <string>
#include <vector>
namespace foedus {
namespace storage {
namespace array {

// Defines ArrayStorage methods so that we can inline implementation calls
bool        ArrayStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        ArrayStorage::exists()           const  { return pimpl_->exist_; }
uint16_t    ArrayStorage::get_payload_size() const  { return pimpl_->metadata_.payload_size_; }
ArrayOffset ArrayStorage::get_array_size()   const  { return pimpl_->metadata_.array_size_; }
StorageId   ArrayStorage::get_id()           const  { return pimpl_->metadata_.id_; }
const std::string& ArrayStorage::get_name()  const  { return pimpl_->metadata_.name_; }
const Metadata* ArrayStorage::get_metadata() const  { return &pimpl_->metadata_; }

ErrorStack ArrayStorage::get_record(thread::Thread* context, ArrayOffset offset,
          void *payload, uint16_t payload_offset, uint16_t payload_count) {
  return pimpl_->get_record(context, offset, payload, payload_offset, payload_count);
}

template <typename T>
ErrorStack ArrayStorage::get_record_primitive(thread::Thread* context, ArrayOffset offset,
          T *payload, uint16_t payload_offset) {
  return pimpl_->get_record_primitive<T>(context, offset, payload, payload_offset);
}

ErrorStack ArrayStorage::overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) {
  return pimpl_->overwrite_record(context, offset, payload, payload_offset, payload_count);
}

template <typename T>
ErrorStack ArrayStorage::overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
          T payload, uint16_t payload_offset) {
  return pimpl_->overwrite_record_primitive<T>(context, offset, payload, payload_offset);
}

template <typename T>
ErrorStack ArrayStorage::increment_record(thread::Thread* context, ArrayOffset offset,
          T* value, uint16_t payload_offset) {
  return pimpl_->increment_record<T>(context, offset, value, payload_offset);
}

/**
 * Calculate leaf/interior pages we need.
 * @return index=level.
 */
std::vector<uint64_t> calculate_required_pages(uint64_t array_size, uint16_t payload) {
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

ArrayStoragePimpl::ArrayStoragePimpl(Engine* engine, ArrayStorage* holder,
                   const ArrayMetadata &metadata, bool create)
  : engine_(engine), holder_(holder), metadata_(metadata), root_page_(nullptr), exist_(!create),
    records_in_leaf_(kDataSize / (metadata.payload_size_ + kRecordOverhead)),
    leaf_fanout_div_(records_in_leaf_),
    interior_fanout_div_(kInteriorFanout) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
  root_page_pointer_.snapshot_pointer_ = metadata.root_page_id_;
  root_page_pointer_.volatile_pointer_.word = 0;
}

ErrorStack ArrayStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an array-storage " << *holder_ << " exists=" << exist_;
  uint16_t payload_size_aligned = (assorted::align8(metadata_.payload_size_));
  pages_ = calculate_required_pages(metadata_.array_size_, payload_size_aligned);
  levels_ = pages_.size();
  offset_intervals_.push_back(kDataSize / (payload_size_aligned + kRecordOverhead));
  for (uint8_t level = 1; level < levels_; ++level) {
    offset_intervals_.push_back(offset_intervals_[level - 1] * kInteriorFanout);
  }
  for (uint8_t level = 0; level < levels_; ++level) {
    LOG(INFO) << "Level-" << static_cast<int>(level) << " pages=" << pages_[level]
      << " interval=" << offset_intervals_[level];
  }

  if (exist_) {
    // initialize root_page_
  }
  return kRetOk;
}

void ArrayStoragePimpl::release_pages_recursive(
  memory::PageReleaseBatch* batch, ArrayPage* page, VolatilePagePointer volatile_page_id) {
  ASSERT_ND(volatile_page_id.components.offset != 0);
  if (!page->is_leaf()) {
    const memory::GlobalPageResolver& page_resolver
      = engine_->get_memory_manager().get_global_page_resolver();
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

  Epoch initial_epoch = engine_->get_xct_manager().get_current_global_epoch();
  LOG(INFO) << "Newly creating an array-storage "  << *holder_ << " as epoch=" << initial_epoch;

  // TODO(Hideaki) This part must handle the case where RAM < Array Size
  // So far, we just crash in RoundRobinPageGrabBatch::grab().

  // we create from left, keeping cursors on each level.
  // first, create the left-most in each level
  // All of the following, index=level
  std::vector<ArrayPage*> current_pages;
  std::vector<VolatilePagePointer> current_pages_ids;
  std::vector<uint16_t> current_records;
  // we grab free page in round-robbin fashion.
  const memory::GlobalPageResolver& page_resolver = context->get_global_page_resolver();
  memory::RoundRobinPageGrabBatch grab_batch(engine_);
  for (uint8_t level = 0; level < levels_; ++level) {
    VolatilePagePointer page_pointer = grab_batch.grab();
    ASSERT_ND(page_pointer.components.offset != 0);
    ArrayPage* page = reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(page_pointer));

    ArrayRange range(0, offset_intervals_[level]);
    if (range.end_ > metadata_.array_size_) {
      ASSERT_ND(level == levels_ - 1);
      range.end_ = metadata_.array_size_;
    }
    page->initialize_data_page(initial_epoch, metadata_.id_,
                   metadata_.payload_size_, level, range);

    current_pages.push_back(page);
    current_pages_ids.push_back(page_pointer);
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
  for (uint64_t leaf = 1; leaf < pages_[0]; ++leaf) {
    VolatilePagePointer page_pointer = grab_batch.grab();
    ASSERT_ND(page_pointer.components.offset != 0);
    ArrayPage* page = reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(page_pointer));

    ArrayRange range(current_pages[0]->get_array_range().end_,
             current_pages[0]->get_array_range().end_ + offset_intervals_[0]);
    if (range.end_ > metadata_.array_size_) {
      range.end_ = metadata_.array_size_;
    }
    page->initialize_data_page(initial_epoch, metadata_.id_, metadata_.payload_size_, 0, range);
    current_pages[0] = page;
    current_pages_ids[0] = page_pointer;
    // current_records[0] is always 0

    // push it up to parent, potentially up to root
    for (uint8_t level = 1; level < levels_; ++level) {
      if (current_records[level] == kInteriorFanout) {
        VLOG(2) << "leaf=" << leaf << ", interior level=" << static_cast<int>(level);
        VolatilePagePointer interior_pointer = grab_batch.grab();
        ASSERT_ND(interior_pointer.components.offset != 0);
        ArrayPage* interior_page = reinterpret_cast<ArrayPage*>(
          page_resolver.resolve_offset(interior_pointer));
        ArrayRange interior_range(current_pages[level]->get_array_range().end_,
             current_pages[level]->get_array_range().end_ + offset_intervals_[level]);
        if (range.end_ > metadata_.array_size_) {
          range.end_ = metadata_.array_size_;
        }
        interior_page->initialize_data_page(
          initial_epoch, metadata_.id_, metadata_.payload_size_, level, interior_range);

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

inline ErrorCode ArrayStoragePimpl::locate_record(
  thread::Thread* context, ArrayOffset offset, Record** out) {
  ASSERT_ND(is_initialized());
  ASSERT_ND(offset < metadata_.array_size_);
  uint16_t index = 0;
  ArrayPage* page = nullptr;
  ErrorCode code = lookup(context, offset, &page, &index);
  if (code != kErrorCodeOk) {
    return code;
  }
  ASSERT_ND(page);
  ASSERT_ND(page->is_leaf());
  ASSERT_ND(page->get_array_range().contains(offset));
  *out = page->get_leaf_record(index);
  return kErrorCodeOk;
}

inline ErrorStack ArrayStoragePimpl::get_record(thread::Thread* context, ArrayOffset offset,
          void *payload, uint16_t payload_offset, uint16_t payload_count) {
  ASSERT_ND(payload_offset + payload_count <= metadata_.payload_size_);
  Record *record = nullptr;
  WRAP_ERROR_CODE(locate_record(context, offset, &record));
  WRAP_ERROR_CODE(context->get_current_xct().read_record(holder_, record,
                            payload, payload_offset, payload_count));
  return kRetOk;
}

template <typename T>
ErrorStack ArrayStoragePimpl::get_record_primitive(thread::Thread* context, ArrayOffset offset,
          T *payload, uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= metadata_.payload_size_);
  Record *record = nullptr;
  WRAP_ERROR_CODE(locate_record(context, offset, &record));
  WRAP_ERROR_CODE(context->get_current_xct().read_record_primitive<T>(holder_, record,
                                    payload, payload_offset));
  return kRetOk;
}

inline ErrorStack ArrayStoragePimpl::overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) {
  ASSERT_ND(payload_offset + payload_count <= metadata_.payload_size_);
  Record *record = nullptr;
  WRAP_ERROR_CODE(locate_record(context, offset, &record));

  // write out log
  uint16_t log_length = OverwriteLogType::calculate_log_length(payload_count);
  OverwriteLogType* log_entry = reinterpret_cast<OverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, offset, payload, payload_offset, payload_count);
  WRAP_ERROR_CODE(context->get_current_xct().add_to_write_set(holder_, record, log_entry));
  return kRetOk;
}

template <typename T>
ErrorStack ArrayStoragePimpl::overwrite_record_primitive(
      thread::Thread* context, ArrayOffset offset, T payload, uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= metadata_.payload_size_);
  Record *record = nullptr;
  WRAP_ERROR_CODE(locate_record(context, offset, &record));

  // write out log
  uint16_t log_length = OverwriteLogType::calculate_log_length(sizeof(T));
  OverwriteLogType* log_entry = reinterpret_cast<OverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate_primitive<T>(metadata_.id_, offset, payload, payload_offset);
  WRAP_ERROR_CODE(context->get_current_xct().add_to_write_set(holder_, record, log_entry));
  return kRetOk;
}

template <typename T>
ErrorStack ArrayStoragePimpl::increment_record(
      thread::Thread* context, ArrayOffset offset, T* value, uint16_t payload_offset) {
  ASSERT_ND(payload_offset + sizeof(T) <= metadata_.payload_size_);
  Record *record = nullptr;
  WRAP_ERROR_CODE(locate_record(context, offset, &record));

  // this is get_record + overwrite_record
  T old_value;
  WRAP_ERROR_CODE(context->get_current_xct().read_record_primitive<T>(
    holder_, record, &old_value, payload_offset));
  *value += old_value;
  uint16_t log_length = OverwriteLogType::calculate_log_length(sizeof(T));
  OverwriteLogType* log_entry = reinterpret_cast<OverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate_primitive<T>(metadata_.id_, offset, *value, payload_offset);
  WRAP_ERROR_CODE(context->get_current_xct().add_to_write_set(holder_, record, log_entry));
  return kRetOk;
}

inline ArrayStoragePimpl::LookupRoute ArrayStoragePimpl::find_route(ArrayOffset offset) const {
  LookupRoute ret;
  ArrayOffset old = offset;
  offset = leaf_fanout_div_.div64(offset);
  ret.route[0] = old - offset * records_in_leaf_;
  for (uint8_t level = 1; level < levels_ - 1; ++level) {
    old = offset;
    offset = interior_fanout_div_.div64(offset);
    ret.route[level] = old - offset * kInteriorFanout;
  }
  if (levels_ > 1) {
    // the last level is done manually because we don't need any division there
    ASSERT_ND(offset < kInteriorFanout);
    ret.route[levels_ - 1] = offset;
  }

  return ret;
}


inline ErrorCode ArrayStoragePimpl::lookup(thread::Thread* context, ArrayOffset offset,
                      ArrayPage** out, uint16_t *index) {
  ASSERT_ND(is_initialized());
  ASSERT_ND(offset < metadata_.array_size_);
  ASSERT_ND(out);
  ASSERT_ND(index);
  ArrayPage* current_page = root_page_;
  ASSERT_ND(current_page->get_array_range().contains(offset));

  // use efficient division to determine the route.
  // this code originally used simple std::lldiv(), which caused 20% of CPU cost in read-only
  // experiment. wow. The code below now costs only 6%.
  LookupRoute route = find_route(offset);
  const memory::GlobalPageResolver& page_resolver = context->get_global_page_resolver();
  for (uint8_t level = levels_ - 1; level > 0; --level) {
    ASSERT_ND(current_page->get_array_range().contains(offset));
    DualPagePointer& pointer = current_page->get_interior_record(route.route[level]);
    // TODO(Hideaki) Add to Node-set (?)
    if (pointer.volatile_pointer_.components.offset == 0) {
      // TODO(Hideaki) Read the page from cache.
      return kErrorCodeNotimplemented;
    } else {
      current_page = reinterpret_cast<ArrayPage*>(
        page_resolver.resolve_offset(pointer.volatile_pointer_));
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
#define EXPLICIT_INSTANTIATION_GET(x) template ErrorStack ArrayStorage::get_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x *payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET);

#define EXPLICIT_INSTANTIATION_OV(x) template ErrorStack\
  ArrayStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_OV);

#define EXPLICIT_INSTANTIATION_INC(x) template ErrorStack ArrayStorage::increment_record< x > \
  (thread::Thread* context, ArrayOffset offset, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_INC);

#define EXPLICIT_INSTANTIATION_GET_IMPL(x) template ErrorStack \
  ArrayStoragePimpl::get_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x *payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_IMPL);

#define EXPLICIT_INSTANTIATION_GET_OV_IMPL(x) template ErrorStack \
  ArrayStoragePimpl::overwrite_record_primitive< x > \
  (thread::Thread* context, ArrayOffset offset, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_OV_IMPL);

#define EXPLICIT_INSTANTIATION_GET_INC_IMPL(x) template ErrorStack \
  ArrayStoragePimpl::increment_record< x > \
  (thread::Thread* context, ArrayOffset offset, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_INC_IMPL);
// @endcond

}  // namespace array
}  // namespace storage
}  // namespace foedus
