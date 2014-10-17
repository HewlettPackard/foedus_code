/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_composer_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace array {

///////////////////////////////////////////////////////////////////////
///
///  ArrayComposer methods
///
///////////////////////////////////////////////////////////////////////
ArrayComposer::ArrayComposer(Composer *parent)
  : engine_(parent->get_engine()),
    storage_id_(parent->get_storage_id()),
    storage_(engine_, storage_id_) {
  ASSERT_ND(storage_.exists());
}

ErrorStack ArrayComposer::compose(const Composer::ComposeArguments& args) {
  VLOG(0) << to_string() << " composing with " << args.log_streams_count_ << " streams.";
  debugging::StopWatch stop_watch;

  args.work_memory_->assure_capacity(sizeof(ArrayStreamStatus) * args.log_streams_count_);
  ArrayComposeContext context(
    engine_,
    storage_id_,
    args.snapshot_writer_,
    args.previous_snapshot_files_,
    args.log_streams_,
    args.log_streams_count_,
    args.work_memory_,
    args.root_info_page_);
  CHECK_ERROR(context.execute());

  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

ErrorStack ArrayComposer::construct_root(const Composer::ConstructRootArguments& args) {
  // compose() created root_info_pages that contain pointers to fill in the root page,
  // so we just find non-zero entry and copy it to root page.
  ArrayStorage storage(engine_, storage_id_);

  uint8_t levels = storage.get_levels();
  uint16_t payload_size = storage.get_payload_size();
  snapshot::SnapshotId new_snapshot_id = args.snapshot_writer_->get_snapshot_id();
  if (levels == 1U) {
    // if it's single-page array, we have already created the root page in compose().
    ASSERT_ND(args.root_info_pages_count_ == 1U);
    const ArrayRootInfoPage* casted
      = reinterpret_cast<const ArrayRootInfoPage*>(args.root_info_pages_[0]);
    ASSERT_ND(casted->pointers_[0] != 0);
    *args.new_root_page_pointer_ = casted->pointers_[0];
  } else {
    ArrayPage* root_page = reinterpret_cast<ArrayPage*>(args.snapshot_writer_->get_page_base());
    SnapshotPagePointer page_id = storage.get_metadata()->root_snapshot_page_id_;
    SnapshotPagePointer new_page_id = args.snapshot_writer_->get_next_page_id();
    *args.new_root_page_pointer_ = new_page_id;
    if (page_id != 0) {
      WRAP_ERROR_CODE(args.previous_snapshot_files_->read_page(page_id, root_page));
      ASSERT_ND(root_page->header().storage_id_ == storage_id_);
      ASSERT_ND(root_page->header().page_id_ == page_id);
      root_page->header().page_id_ = new_page_id;
    } else {
      uint64_t root_interval = LookupRouteFinder(levels, payload_size).get_records_in_leaf();
      for (uint8_t level = 1; level < levels; ++level) {
        root_interval *= kInteriorFanout;
      }
      ArrayRange range(0, root_interval);
      if (range.end_ > storage_.get_array_size()) {
        range.end_ = storage_.get_array_size();
      }
      root_page->initialize_snapshot_page(
        storage_id_,
        new_page_id,
        payload_size,
        levels - 1,
        range);
    }

    // overwrite pointers with root_info_pages.
    for (uint32_t i = 0; i < args.root_info_pages_count_; ++i) {
      const ArrayRootInfoPage* casted
        = reinterpret_cast<const ArrayRootInfoPage*>(args.root_info_pages_[i]);
      for (uint16_t j = 0; j < kInteriorFanout; ++j) {
        SnapshotPagePointer pointer = casted->pointers_[j];
        if (pointer != 0) {
          ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(pointer) == new_snapshot_id);
          DualPagePointer& record = root_page->get_interior_record(j);
          // partitioning has no overlap, so this must be the only overwriting pointer
          ASSERT_ND(record.snapshot_pointer_ == 0 ||
            extract_snapshot_id_from_snapshot_pointer(record.snapshot_pointer_)
              != new_snapshot_id);
          record.snapshot_pointer_ = pointer;
        }
      }
    }

    WRAP_ERROR_CODE(args.snapshot_writer_->dump_pages(0, 1));
    ASSERT_ND(args.snapshot_writer_->get_next_page_id() == new_page_id + 1ULL);
  }
  return kRetOk;
}


std::string ArrayComposer::to_string() const {
  return std::string("ArrayComposer-") + std::to_string(storage_id_);
}

ErrorStack ArrayComposer::replace_pointers(const Composer::ReplacePointersArguments& args) {
  return ArrayStorage(engine_, storage_id_).replace_pointers(args);
}

///////////////////////////////////////////////////////////////////////
///
///  ArrayStreamStatus methods
///
///////////////////////////////////////////////////////////////////////
void ArrayStreamStatus::init(snapshot::SortedBuffer* stream) {
  stream_ = stream;
  stream_->assert_checks();
  buffer_ = stream->get_buffer();
  buffer_size_ = stream->get_buffer_size();
  cur_absolute_pos_ = stream->get_cur_block_abosulte_begin();
  // this is the initial read of this block, so we are sure cur_block_abosulte_begin is the window
  ASSERT_ND(cur_absolute_pos_ >= stream->get_offset());
  cur_relative_pos_ = cur_absolute_pos_ - stream->get_offset();
  end_absolute_pos_ = stream->get_cur_block_abosulte_end();
  ended_ = false;
  read_entry();
}


inline ErrorCode ArrayStreamStatus::next() {
  ASSERT_ND(!ended_);
  cur_absolute_pos_ += cur_length_;
  cur_relative_pos_ += cur_length_;
  if (UNLIKELY(cur_absolute_pos_ >= end_absolute_pos_)) {
    ended_ = true;
    return kErrorCodeOk;
  } else if (UNLIKELY(cur_relative_pos_ >= buffer_size_)) {
    CHECK_ERROR_CODE(stream_->wind(cur_absolute_pos_));
    cur_relative_pos_ = stream_->to_relative_pos(cur_absolute_pos_);
  }
  read_entry();
  return kErrorCodeOk;
}
inline void ArrayStreamStatus::read_entry() {
  const ArrayCommonUpdateLogType* entry = get_entry();
  ASSERT_ND(entry->header_.get_type() == log::kLogCodeArrayOverwrite
    || entry->header_.get_type() == log::kLogCodeArrayIncrement);
  ASSERT_ND(entry->header_.log_length_ > 0);
  cur_value_ = entry->offset_;
  cur_xct_id_ = entry->header_.xct_id_;
  cur_length_ = entry->header_.log_length_;
}

inline const ArrayCommonUpdateLogType* ArrayStreamStatus::get_entry() const {
  return reinterpret_cast<const ArrayCommonUpdateLogType*>(buffer_ + cur_relative_pos_);
}

///////////////////////////////////////////////////////////////////////
///
///  ArrayComposeContext methods
///
///////////////////////////////////////////////////////////////////////
ArrayComposeContext::ArrayComposeContext(
  Engine*                           engine,
  StorageId                         storage_id,
  snapshot::SnapshotWriter*         snapshot_writer,
  cache::SnapshotFileSet*           previous_snapshot_files,
  snapshot::SortedBuffer* const*    log_streams,
  uint32_t                          log_streams_count,
  memory::AlignedMemory*            work_memory,
  Page*                             root_info_page)
  : engine_(engine),
    storage_id_(storage_id),
    storage_(engine, storage_id),
    snapshot_writer_(snapshot_writer),
    previous_snapshot_files_(previous_snapshot_files),
    root_info_page_(reinterpret_cast<ArrayRootInfoPage*>(root_info_page)),
    // so far this is the only use of work_memory (an array of pointers) in this composer
    inputs_(reinterpret_cast<ArrayStreamStatus*>(work_memory->get_block())),
    inputs_count_(log_streams_count) {
  for (uint32_t i = 0; i < log_streams_count; ++i) {
    inputs_[i].init(log_streams[i]);
  }
}

ErrorStack ArrayComposeContext::execute() {
  std::memset(root_info_page_, 0, kPageSize);
  root_info_page_->header_.storage_id_ = storage_id_;
  CHECK_ERROR(init_more());

  // TODO(Hideaki) in case the storage's current root snapshot page pointer is null
  // and not all the records are modified, we must create zero-cleared snapshot pages even though
  // there are no logs. So far this does not happen in our experiments/testcases, but it will.
  while (ended_inputs_count_ < inputs_count_) {
    const ArrayCommonUpdateLogType* entry = get_next_entry();
    Record* record = cur_path_[0]->get_leaf_record(next_route_.route[0], payload_size_);
    ASSERT_ND(cur_path_[0]->get_array_range().contains(entry->offset_));
    ASSERT_ND(entry->offset_ - cur_path_[0]->get_array_range().begin_ == next_route_.route[0]);
    if (entry->header_.get_type() == log::kLogCodeArrayOverwrite) {
      const ArrayOverwriteLogType* casted = reinterpret_cast<const ArrayOverwriteLogType*>(entry);
      casted->apply_record(nullptr, storage_id_, &record->owner_id_, record->payload_);
    } else {
      ASSERT_ND(entry->header_.get_type() == log::kLogCodeArrayIncrement);
      const ArrayIncrementLogType* casted = reinterpret_cast<const ArrayIncrementLogType*>(entry);
      casted->apply_record(nullptr, storage_id_, &record->owner_id_, record->payload_);
    }
    WRAP_ERROR_CODE(advance());
  }

  if (levels_ <= 1U) {
    // this storage has only one page. This is very special and trivial.
    // we process this case separately.
    ASSERT_ND(allocated_pages_ == 1U);
    ASSERT_ND(allocated_intermediates_ == 0);
    ASSERT_ND(cur_path_[0] == page_base_);
    ASSERT_ND(snapshot_writer_->get_next_page_id() == page_base_[0].header().page_id_);
    WRAP_ERROR_CODE(snapshot_writer_->dump_pages(0, 1));
    root_info_page_->pointers_[0] = page_base_[0].header().page_id_;
  } else {
    CHECK_ERROR(finalize());
  }
  return kRetOk;
}

ErrorStack ArrayComposeContext::finalize() {
  ASSERT_ND(levels_ > 1U);
  // flush the main buffer. now we finalized all leaf pages
  if (allocated_pages_ > 0) {
    // dump everything in main buffer (intermediate pages are kept)
    WRAP_ERROR_CODE(snapshot_writer_->dump_pages(0, allocated_pages_));
    ASSERT_ND(snapshot_writer_->get_next_page_id()
      == page_base_[0].header().page_id_ + allocated_pages_);
    ASSERT_ND(snapshot_writer_->get_next_page_id()
      == page_base_[allocated_pages_ - 1].header().page_id_ + 1ULL);
    allocated_pages_ = 0;
  }

  // intermediate pages are different animals.
  // we store them in a separate buffer, and now finally we can get their page IDs.
  // Until now, we used indexes in intermediate buffer as page ID, storing them in
  // page ID header and volatile pointer. now let's convert all of them to be final page ID.
  ArrayPage* root_page = intermediate_base_;
  ASSERT_ND(root_page == cur_path_[levels_ - 1]);
  ASSERT_ND(root_page->get_level() == levels_ - 1);

  // base_pointer + offset in intermediate buffer will be the new page ID.
  // -1 because we don't write out root page.
  const SnapshotPagePointer base_pointer = snapshot_writer_->get_next_page_id() - 1ULL;
  for (uint32_t i = 1; i < allocated_intermediates_; ++i) {
    SnapshotPagePointer new_page_id = base_pointer + i;
    ArrayPage* page = intermediate_base_ + i;
    ASSERT_ND(page->header().page_id_ == i);
    ASSERT_ND(page->get_level() > 0);
    ASSERT_ND(page->get_level() < levels_ - 1U);
    page->header().page_id_ = new_page_id;
    if (page->get_level() > 1U) {
      for (uint16_t j = 0; j < kInteriorFanout; ++j) {
        DualPagePointer& pointer = page->get_interior_record(j);
        if (pointer.volatile_pointer_.word != 0) {
          ASSERT_ND(pointer.volatile_pointer_.components.flags == kFlagIntermediatePointer);
          ASSERT_ND(pointer.volatile_pointer_.components.offset < allocated_intermediates_);
          pointer.snapshot_pointer_ = base_pointer + pointer.volatile_pointer_.components.offset;
          ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
          pointer.volatile_pointer_.word = 0;
        }
      }
    }
  }

  // we do not write out root page. rather we just put an equivalent information to the
  // root_info_page. construct_root() will combine all composers' output later.
  snapshot_writer_->dump_intermediates(1, allocated_intermediates_ - 1);
  for (uint16_t j = 0; j < kInteriorFanout; ++j) {
    DualPagePointer& pointer = root_page->get_interior_record(j);
    if (pointer.snapshot_pointer_ != 0) {
      // we already have snapshot pointers because it points to leaf pages. (2 level array)
      ASSERT_ND(root_page->get_level() == 1U);
      ASSERT_ND(pointer.volatile_pointer_.is_null());
      ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
      root_info_page_->pointers_[j] = pointer.snapshot_pointer_;
    } else if (pointer.volatile_pointer_.word != 0) {
      ASSERT_ND(pointer.volatile_pointer_.components.flags == kFlagIntermediatePointer);
      ASSERT_ND(pointer.volatile_pointer_.components.offset < allocated_intermediates_);
      SnapshotPagePointer page_id = base_pointer + pointer.volatile_pointer_.components.offset;
      ASSERT_ND(verify_snapshot_pointer(page_id));
      root_info_page_->pointers_[j] = page_id;
      pointer.snapshot_pointer_ = page_id;
      pointer.volatile_pointer_.word = 0;
    }
  }
  return kRetOk;
}

ErrorStack ArrayComposeContext::init_more() {
  ArrayStorage storage = engine_->get_storage_manager()->get_array(storage_id_);
  payload_size_ = storage.get_payload_size();
  levels_ = storage.get_levels();
  route_finder_ = LookupRouteFinder(levels_, payload_size_);
  previous_root_page_pointer_ = storage.get_metadata()->root_snapshot_page_id_;
  offset_intervals_[0] = route_finder_.get_records_in_leaf();
  for (uint8_t level = 1; level < levels_; ++level) {
    offset_intervals_[level] = offset_intervals_[level - 1] * kInteriorFanout;
  }

  ended_inputs_count_ = 0;

  // let's load the first pages. what's the first key/xct_id?
  next_input_ = inputs_count_;
  next_key_ = 0xFFFFFFFFFFFFFFFFULL;
  next_xct_id_.set_epoch_int(Epoch::kEpochIntHalf - 1U);
  next_xct_id_.set_ordinal(0xFFFFU);
  for (uint32_t i = 0; i < inputs_count_; ++i) {
    if (inputs_[i].cur_value_ < next_key_ || (
        inputs_[i].cur_value_ == next_key_ &&
        inputs_[i].cur_xct_id_.before(next_xct_id_))) {
      next_input_ = i;
      next_key_ = inputs_[i].cur_value_;
      next_xct_id_ = inputs_[i].cur_xct_id_;
    }
  }
  ASSERT_ND(next_input_ < inputs_count_);
  next_route_ = route_finder_.find_route_and_switch(
    next_key_,
    &next_page_starts_,
    &next_page_ends_);

  allocated_pages_ = 0;
  allocated_intermediates_ = 0;
  page_base_ = reinterpret_cast<ArrayPage*>(snapshot_writer_->get_page_base());
  max_pages_ = snapshot_writer_->get_page_size();
  intermediate_base_ = reinterpret_cast<ArrayPage*>(snapshot_writer_->get_intermediate_base());
  max_intermediates_ = snapshot_writer_->get_intermediate_size();

  // set cur_path
  cur_route_.word = next_route_.word;
  std::memset(cur_path_, 0, sizeof(cur_path_));
  if (previous_root_page_pointer_ != 0) {
    CHECK_ERROR(init_context_cur_path());
  } else {
    init_context_empty_cur_path();
  }
  ASSERT_ND(verify_cur_path());
  return kRetOk;
}

ErrorStack ArrayComposeContext::init_context_cur_path() {
  ASSERT_ND(allocated_intermediates_ == 0);
  ASSERT_ND(allocated_pages_ == 0);
  ASSERT_ND(previous_root_page_pointer_ != 0);
  // there is a previous snapshot. read the previous pages
  SnapshotPagePointer old_page_id = previous_root_page_pointer_;
  for (uint8_t level = levels_ - 1;; --level) {  // be careful. unsigned. "level>=0" is wrong
    ASSERT_ND(old_page_id > 0);
    SnapshotPagePointer new_page_id;
    ArrayPage* page;
    if (level > 0) {
      // intermediate page. allocated in intermediate buffer.
      // in this case, we tentatively assign index in the buffer as page ID.
      // we don't know the final page ID yet.
      page = intermediate_base_ + allocated_intermediates_;
      new_page_id = allocated_intermediates_;
      ++allocated_intermediates_;
    } else {
      // leaf page. allocated in main buffer.
      // in this case, we know what the new page ID will be.
      page = page_base_ + allocated_pages_;
      new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
      ASSERT_ND(verify_snapshot_pointer(new_page_id));
      ++allocated_pages_;
    }
    cur_path_[level] = page;

    WRAP_ERROR_CODE(previous_snapshot_files_->read_page(old_page_id, page));
    ASSERT_ND(page->header().storage_id_ == storage_id_);
    ASSERT_ND(page->header().page_id_ == old_page_id);
    ASSERT_ND(page->get_level() == level);
    page->header().page_id_ = new_page_id;
    if (level > 0) {
      DualPagePointer& pointer = page->get_interior_record(next_route_.route[level]);
      old_page_id = pointer.snapshot_pointer_;
      if (level == 1) {
        // next page is leaf node, so we know what its new page ID will be
        pointer.snapshot_pointer_ = snapshot_writer_->get_next_page_id() + allocated_pages_;
        ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
      } else {
        // next page is still intermediate page. In this case, we use volatile page
        // to denote that we also have the pointed page in intermediate buffer.
        pointer.volatile_pointer_.components.flags = kFlagIntermediatePointer;
        pointer.volatile_pointer_.components.offset = allocated_intermediates_ + 1;
      }
    } else {
      break;
    }
  }
  return kRetOk;
}
void ArrayComposeContext::init_context_empty_cur_path() {
  ASSERT_ND(allocated_intermediates_ == 0);
  ASSERT_ND(allocated_pages_ == 0);
  ASSERT_ND(previous_root_page_pointer_ == 0);
  // allocate empty intermediate pages
  for (uint8_t level = levels_ - 1; level > 0; --level) {
    ArrayPage* page = intermediate_base_ + allocated_intermediates_;
    SnapshotPagePointer new_page_id = allocated_intermediates_;
    ++allocated_intermediates_;
    ArrayRange range = calculate_array_range(next_route_, level);
    page->initialize_snapshot_page(
      storage_id_,
      new_page_id,
      payload_size_,
      level,
      range);
    cur_path_[level] = page;
    DualPagePointer& pointer = page->get_interior_record(next_route_.route[level]);
    if (level == 1) {
      pointer.snapshot_pointer_ = snapshot_writer_->get_next_page_id() + allocated_pages_;
      ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
    } else {
      pointer.volatile_pointer_.components.flags = kFlagIntermediatePointer;
      pointer.volatile_pointer_.components.offset = allocated_intermediates_ + 1;
    }
  }
  {
    // allocate empty leaf page
    ArrayPage* page = page_base_ + allocated_pages_;
    SnapshotPagePointer new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
    ASSERT_ND(verify_snapshot_pointer(new_page_id));
    ++allocated_pages_;
    ArrayRange range = calculate_array_range(next_route_, 0);
    page->initialize_snapshot_page(
      storage_id_,
      new_page_id,
      payload_size_,
      0,
      range);
    cur_path_[0] = page;
  }
  // TODO(Hideaki) In this case, if there some pages that don't have logs, they will be still
  // non-existent in snapshot. In that case we should create all pages. Just zero-out.
}

inline ErrorCode ArrayComposeContext::advance() {
  ASSERT_ND(!inputs_[next_input_].ended_);
  // advance the chosen stream
  CHECK_ERROR_CODE(inputs_[next_input_].next());
  if (inputs_[next_input_].ended_) {
    ++ended_inputs_count_;
    if (ended_inputs_count_ >= inputs_count_) {
      ASSERT_ND(ended_inputs_count_ == inputs_count_);
      return kErrorCodeOk;
    }
  }

  // TODO(Hideaki): winner/loser tree to speed this up. binary or n-ary?
  // Or, batched sort with marking end of each buffer.
  // Also, we should treat inputs_count_==1 case more drastically different as an optimization.
  // It's not that rare if partitioning is working well.
  if (inputs_count_ == 1) {
    next_input_ = 0;
    next_key_ = inputs_[0].cur_value_;
    next_xct_id_ = inputs_[0].cur_xct_id_;
  } else {
    next_input_ = inputs_count_;
    next_key_ = 0xFFFFFFFFFFFFFFFFULL;
    next_xct_id_.data_ = 0xFFFFFFFFFFFFFFFFULL;
    for (uint32_t i = 0; i < inputs_count_; ++i) {
      // TODO(Hideaki): rather than checking ended each time, we should re-allocate the array when
      // some stream is ended.
      if (!inputs_[i].ended_) {
        if (inputs_[i].cur_value_ < next_key_ || (
            // "ordinal ==" is not an issue. If there is a conflict, we did issue a new ordinal.
            // so, it can't happen.
            inputs_[i].cur_value_ == next_key_ &&
            inputs_[i].cur_xct_id_.before(next_xct_id_))) {
          next_input_ = i;
          next_key_ = inputs_[i].cur_value_;
          next_xct_id_ = inputs_[i].cur_xct_id_;
        }
      }
    }
    ASSERT_ND(next_input_ < inputs_count_);
  }
  bool switched = update_next_route();
  if (switched) {
    return update_cur_path();
  } else {
    return kErrorCodeOk;
  }
}

inline bool ArrayComposeContext::update_next_route() {
  if (next_key_ < next_page_ends_) {
    ASSERT_ND(next_key_ >= next_page_starts_);
    next_route_.route[0] = next_key_ - next_page_starts_;
    ASSERT_ND(next_route_.word == route_finder_.find_route(next_key_).word);
    return false;
  } else {
    next_route_ = route_finder_.find_route_and_switch(
      next_key_,
      &next_page_starts_,
      &next_page_ends_);
    return true;
  }
}

ErrorCode ArrayComposeContext::update_cur_path() {
  ASSERT_ND(verify_cur_path());
  bool switched = false;
  // Example: levels = 3 (root-intermediate-leaf)
  // cur[2] = 10, cur[1] = 30, cur[0] = 43
  // next[2] = 10, next[1] = 31, next[0] = 2
  // so, it switches at "level=1".
  // cur_path[2] is root page, cur_path[1] is 11'th pointer in root page,
  // cur_path[0] is 31'th pointer in cur_path[1] page.
  // here, we are switching cur_path[0] only.
  for (uint8_t level = levels_ - 1U; level > 0U; --level) {
    // we don't care changes in route[0] (record ordinals in page)
    if (!switched && cur_route_.route[level] == next_route_.route[level]) {
      // skip non-changed path. most likely only the leaf has changed.
      continue;
    }

    ASSERT_ND(level >= 1U);
    uint8_t page_level = level - 1U;  // level of the page (not route) that contains the route
    switched = true;
    // page switched! we have to allocate a new page and point to it.
    ArrayPage* parent = cur_path_[level];
    ASSERT_ND(parent->get_level() == level);
    cur_route_.route[level] = next_route_.route[level];
    DualPagePointer& pointer = parent->get_interior_record(cur_route_.route[level]);
    ASSERT_ND(pointer.volatile_pointer_.word == 0);
    SnapshotPagePointer old_page_id = pointer.snapshot_pointer_;
    ASSERT_ND((previous_root_page_pointer_ != 0 && old_page_id != 0) ||
      (previous_root_page_pointer_ == 0 && old_page_id == 0));

    ArrayPage* page;
    if (page_level > 0U) {
      // we switched an intermediate page
      page = intermediate_base_ + allocated_intermediates_;
      SnapshotPagePointer new_page_id = allocated_intermediates_;
      ++allocated_intermediates_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, page_level, cur_route_, page));
      pointer.volatile_pointer_.components.flags = kFlagIntermediatePointer;
      pointer.volatile_pointer_.components.offset = new_page_id;
    } else {
      // we switched a leaf page. in this case, we might have to flush the buffer
      if (allocated_pages_ >= max_pages_) {
        // dump everything in main buffer (intermediate pages are kept)
        CHECK_ERROR_CODE(snapshot_writer_->dump_pages(0, allocated_pages_));
        ASSERT_ND(snapshot_writer_->get_next_page_id()
          == page_base_[0].header().page_id_ + allocated_pages_);
        ASSERT_ND(snapshot_writer_->get_next_page_id()
          == page_base_[allocated_pages_ - 1].header().page_id_ + 1ULL);
        allocated_pages_ = 0;
      }

      // remember, we can finalize the page ID of leaf pages at this point
      page = page_base_ + allocated_pages_;
      SnapshotPagePointer new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
      ASSERT_ND(verify_snapshot_pointer(new_page_id));
      ++allocated_pages_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, page_level, cur_route_, page));
      pointer.snapshot_pointer_ = new_page_id;
    }
    cur_path_[page_level] = page;
    ASSERT_ND(verify_cur_path());
  }
  return kErrorCodeOk;
}

inline ErrorCode ArrayComposeContext::read_or_init_page(
  SnapshotPagePointer old_page_id,
  SnapshotPagePointer new_page_id,
  uint8_t level,
  LookupRoute route,
  ArrayPage* page) {
  if (old_page_id != 0) {
    ASSERT_ND(previous_root_page_pointer_ != 0);
    CHECK_ERROR_CODE(previous_snapshot_files_->read_page(old_page_id, page));
    ASSERT_ND(page->header().storage_id_ == storage_id_);
    ASSERT_ND(page->header().page_id_ == old_page_id);
    ASSERT_ND(page->get_level() == level);
    page->header().page_id_ = new_page_id;
  } else {
    ASSERT_ND(previous_root_page_pointer_ == 0);
    ArrayRange range = calculate_array_range(route, level);
    page->initialize_snapshot_page(
      storage_id_,
      new_page_id,
      payload_size_,
      level,
      range);
  }
  return kErrorCodeOk;
}

inline const ArrayCommonUpdateLogType* ArrayComposeContext::get_next_entry() const {
  ASSERT_ND(!inputs_[next_input_].ended_);
  const ArrayCommonUpdateLogType* entry = inputs_[next_input_].get_entry();
  ASSERT_ND(entry->offset_ == next_key_);
  ASSERT_ND(entry->header_.xct_id_ == next_xct_id_);
  return entry;
}

inline ArrayRange ArrayComposeContext::calculate_array_range(
  LookupRoute route,
  uint8_t level) const {
  ASSERT_ND(level < levels_);
  ArrayRange range;
  range.begin_ = 0;
  // For example, levels=2, offset_intervals_[0]=100, offset_intervals_[1]=100*253, ArraySize=10000
  // route=[20, 50] (meaning array offset=5020)
  // if level=1, we want to return 0-25300.
  // if level=0, we want to return 5000~5100.
  for (uint8_t i = level + 1; i < levels_; ++i) {
    range.begin_ += offset_intervals_[i - 1] * route.route[i];
  }
  range.end_ = range.begin_ + offset_intervals_[level];
  if (range.end_ > storage_.get_array_size()) {
    range.end_ = storage_.get_array_size();
  }
  return range;
}

bool ArrayComposeContext::verify_cur_path() const {
  for (uint8_t level = 0; level < kMaxLevels; ++level) {
    if (level >= levels_) {
      ASSERT_ND(cur_path_[level] == nullptr);
      continue;
    }
    ASSERT_ND(cur_path_[level]);
    ASSERT_ND(cur_path_[level]->get_level() == level);
    ASSERT_ND(cur_path_[level]->get_storage_id() == storage_id_);
  }
  return true;
}

bool ArrayComposeContext::verify_snapshot_pointer(SnapshotPagePointer pointer) {
  ASSERT_ND(extract_local_page_id_from_snapshot_pointer(pointer) > 0U);
  if (!engine_->is_master()) {
    ASSERT_ND(extract_numa_node_from_snapshot_pointer(pointer)
      == snapshot_writer_->get_numa_node());
  }
  ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(pointer)
    == snapshot_writer_->get_snapshot_id());
  return true;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
