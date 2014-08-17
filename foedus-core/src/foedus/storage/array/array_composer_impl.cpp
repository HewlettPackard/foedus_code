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

namespace foedus {
namespace storage {
namespace array {

ArrayComposer::ArrayComposer(
    Engine *engine,
    const ArrayPartitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    cache::SnapshotFileSet* previous_snapshot_files,
    const snapshot::Snapshot& new_snapshot)
  : Composer(engine, partitioner, snapshot_writer, previous_snapshot_files, new_snapshot),
    storage_casted_(dynamic_cast<ArrayStorage*>(storage_)),
    payload_size_(storage_casted_->get_payload_size()),
    levels_(storage_casted_->get_levels()),
    route_finder_(levels_, payload_size_) {
  ASSERT_ND(partitioner);
  offset_intervals_[0] = route_finder_.get_records_in_leaf();
  for (uint8_t level = 1; level < levels_; ++level) {
    offset_intervals_[level] = offset_intervals_[level - 1] * kInteriorFanout;
  }
}
void ArrayComposer::StreamStatus::init(snapshot::SortedBuffer* stream) {
  stream_ = stream;
  buffer_ = stream->get_buffer();
  buffer_size_ = stream->get_buffer_size();
  cur_absolute_pos_ = stream->get_cur_block_abosulte_begin();
  cur_relative_pos_ = stream->get_offset();
  end_absolute_pos_ = stream->get_cur_block_abosulte_end();
  ended_ = false;
  read_entry();
}

ErrorStack ArrayComposer::compose(
  snapshot::SortedBuffer* const* log_streams,
  uint32_t log_streams_count,
  const memory::AlignedMemorySlice& work_memory,
  Page* root_info_page) {
  VLOG(0) << to_string() << " composing with " << log_streams_count << " streams.";
  debugging::StopWatch stop_watch;

  RootInfoPage* root_info_page_casted = reinterpret_cast<RootInfoPage*>(root_info_page);
  std::memset(root_info_page_casted, 0, kPageSize);
  root_info_page_casted->header_.storage_id_ = storage_id_;

  WRAP_ERROR_CODE(compose_init_context(
    work_memory,
    log_streams,
    log_streams_count));

  while (ended_inputs_count_ < inputs_count_) {
    const ArrayOverwriteLogType* entry = get_next_entry();
    Record* record = cur_path_[0]->get_leaf_record(cur_route_.route[0], payload_size_);
    entry->apply_record(nullptr, storage_casted_, &record->owner_id_, record->payload_);
    WRAP_ERROR_CODE(advance());
  }

  if (levels_ <= 1) {
    // this storage has only one page. This is very special and trivial.
    // we process this case separately.
    ASSERT_ND(allocated_pages_ == 1);
    ASSERT_ND(allocated_intermediates_ == 0);
    ASSERT_ND(cur_path_[0] == page_base_);
    ASSERT_ND(snapshot_writer_->get_next_page_id() == page_base_[0].header().page_id_);
    WRAP_ERROR_CODE(snapshot_writer_->dump_pages(0, 1));
    root_info_page_casted->pointers_[0] = page_base_[0].header().page_id_;
  } else {
    CHECK_ERROR(compose_finalize(root_info_page_casted));
  }

  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

ErrorStack ArrayComposer::compose_finalize(RootInfoPage* root_info_page) {
  ASSERT_ND(levels_ > 1);
  // flush the main buffer. now we finalized all leaf pages
  if (allocated_pages_ > 0) {
    // dump everything in main buffer (intermediate pages are kept)
    WRAP_ERROR_CODE(snapshot_writer_->dump_pages(0, allocated_pages_));
    ASSERT_ND(snapshot_writer_->get_next_page_id()
      == page_base_[0].header().page_id_ + allocated_pages_);
    ASSERT_ND(snapshot_writer_->get_next_page_id()
      == page_base_[allocated_pages_ - 1].header().page_id_ + 1);
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
  const SnapshotPagePointer base_pointer = snapshot_writer_->get_next_page_id() - 1;
  for (uint32_t i = 1; i < allocated_intermediates_; ++i) {
    SnapshotPagePointer new_page_id = base_pointer + i;
    ArrayPage* page = intermediate_base_ + i;
    ASSERT_ND(page->header().page_id_ == i);
    ASSERT_ND(page->get_level() > 0);
    ASSERT_ND(page->get_level() < levels_ - 1);
    page->header().page_id_ = new_page_id;
    if (page->get_level() > 1) {
      for (uint16_t j = 0; j < kInteriorFanout; ++j) {
        DualPagePointer& pointer = page->get_interior_record(j);
        if (pointer.volatile_pointer_.word != 0) {
          ASSERT_ND(pointer.volatile_pointer_.components.flags == kFlagIntermediatePointer);
          ASSERT_ND(pointer.volatile_pointer_.components.offset < allocated_intermediates_);
          pointer.snapshot_pointer_ = base_pointer + pointer.volatile_pointer_.components.offset;
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
    if (pointer.volatile_pointer_.word != 0) {
      ASSERT_ND(pointer.volatile_pointer_.components.flags == kFlagIntermediatePointer);
      ASSERT_ND(pointer.volatile_pointer_.components.offset < allocated_intermediates_);
      SnapshotPagePointer page_id = base_pointer + pointer.volatile_pointer_.components.offset;
      root_info_page->pointers_[j] = page_id;
      pointer.snapshot_pointer_ = page_id;
      pointer.volatile_pointer_.word = 0;
    }
  }

  // TODO(Hideaki): in terms of constructing snapshots, we are done already.
  // however, we must do one more thing for in-memory storage; installing
  // the new pointers to volatile pages and drop child volatile pages if possible.

  return kRetOk;
}

ErrorStack ArrayComposer::construct_root(
  const Page* const* root_info_pages,
  uint32_t root_info_pages_count,
  const memory::AlignedMemorySlice& /*work_memory*/,
  SnapshotPagePointer* new_root_page_pointer) {
  // compose() created root_info_pages that contain pointers to fill in the root page,
  // so we just find non-zero entry and copy it to root page.
  if (levels_ == 1) {
    // if it's single-page array, we have already created the root page in compose().
    ASSERT_ND(root_info_pages_count == 1);
    const RootInfoPage* casted = reinterpret_cast<const RootInfoPage*>(root_info_pages[0]);
    ASSERT_ND(casted->pointers_[0] != 0);
    *new_root_page_pointer = casted->pointers_[0];
  } else {
    ArrayPage* root_page = reinterpret_cast<ArrayPage*>(snapshot_writer_->get_page_base());
    SnapshotPagePointer page_id = storage_->get_metadata()->root_snapshot_page_id_;
    SnapshotPagePointer new_page_id = snapshot_writer_->get_next_page_id();
    *new_root_page_pointer = new_page_id;
    if (page_id != 0) {
      WRAP_ERROR_CODE(previous_snapshot_files_->read_page(page_id, root_page));
      ASSERT_ND(root_page->header().storage_id_ == storage_id_);
      ASSERT_ND(root_page->header().page_id_ == page_id);
      root_page->header().page_id_ = new_page_id;
    } else {
      ArrayRange range(0, offset_intervals_[levels_ - 1]);
      root_page->initialize_snapshot_page(
        storage_id_,
        new_page_id,
        payload_size_,
        levels_ - 1,
        range);
    }

    // overwrite pointers with root_info_pages.
    for (uint32_t i = 0; i < root_info_pages_count; ++i) {
      const RootInfoPage* casted = reinterpret_cast<const RootInfoPage*>(root_info_pages[i]);
      for (uint16_t j = 0; j < kInteriorFanout; ++j) {
        SnapshotPagePointer pointer = casted->pointers_[j];
        if (pointer != 0) {
          ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(pointer) == new_snapshot_id_);
          DualPagePointer& record = root_page->get_interior_record(j);
          record.snapshot_pointer_ = pointer;
          // partitioning has no overlap, so this must be the only overwriting pointer
          ASSERT_ND(record.snapshot_pointer_ == 0 ||
            extract_snapshot_id_from_snapshot_pointer(record.snapshot_pointer_)
              != new_snapshot_id_);
        }
      }
    }

    WRAP_ERROR_CODE(snapshot_writer_->dump_pages(0, 1));
    ASSERT_ND(snapshot_writer_->get_next_page_id() == new_page_id - 1);
  }
  return kRetOk;
}

inline ErrorCode ArrayComposer::StreamStatus::next() {
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
inline void ArrayComposer::StreamStatus::read_entry() {
  const ArrayOverwriteLogType* entry = get_entry();
  ASSERT_ND(entry->header_.get_type() == log::kLogCodeArrayOverwrite);
  ASSERT_ND(entry->header_.log_length_ > 0);
  cur_value_ = entry->offset_;
  cur_xct_id_ = entry->header_.xct_id_;
  cur_length_ = entry->header_.log_length_;
}
inline const ArrayOverwriteLogType* ArrayComposer::StreamStatus::get_entry() const {
  return reinterpret_cast<const ArrayOverwriteLogType*>(buffer_ + cur_relative_pos_);
}

ErrorCode ArrayComposer::compose_init_context(
  const memory::AlignedMemorySlice& work_memory,
  snapshot::SortedBuffer* const* inputs,
  uint32_t inputs_count) {
  // so far this is the only use of work_memory in this composer
  inputs_ = reinterpret_cast<StreamStatus*>(work_memory.get_block());
  inputs_count_ = inputs_count;
  ended_inputs_count_ = 0;
  for (uint32_t i = 0; i < inputs_count; ++i) {
    inputs_[i].init(inputs[i]);
  }

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
    return compose_init_context_cur_path();
  } else {
    compose_init_context_empty_cur_path();
    return kErrorCodeOk;
  }
}

ErrorCode ArrayComposer::compose_init_context_cur_path() {
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
      ++allocated_pages_;
    }
    cur_path_[level] = page;

    CHECK_ERROR_CODE(previous_snapshot_files_->read_page(old_page_id, page));
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
  return kErrorCodeOk;
}
void ArrayComposer::compose_init_context_empty_cur_path() {
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
    } else {
      pointer.volatile_pointer_.components.flags = kFlagIntermediatePointer;
      pointer.volatile_pointer_.components.offset = allocated_intermediates_ + 1;
    }
  }
  {
    // allocate empty leaf page
    ArrayPage* page = page_base_ + allocated_intermediates_;
    SnapshotPagePointer new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
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

inline ErrorCode ArrayComposer::advance() {
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
    update_next_route();
    return kErrorCodeOk;
  }

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
  bool switched = update_next_route();
  if (switched) {
    return update_cur_path();
  } else {
    return kErrorCodeOk;
  }
}

inline bool ArrayComposer::update_next_route() {
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

ErrorCode ArrayComposer::update_cur_path() {
  bool switched = false;
  // Example: levels = 3 (root-intermediate-leaf)
  // cur[2] = 10, cur[1] = 30, cur[0] = 43
  // next[2] = 10, next[1] = 31, next[0] = 2
  // so, it switches at "level=1".
  // cur_path[2] is root page, cur_path[1] is 11'th pointer in root page,
  // cur_path[0] is 31'th pointer in cur_path[1] page.
  // here, we are switching cur_path[0] only.
  for (uint8_t level = levels_ - 1; level > 0; --level) {
    // we don't care changes in route[0] (record ordinals in page)
    if (!switched && cur_route_.route[level] == next_route_.route[level]) {
      // skip non-changed path. most likely only the leaf has changed.
      ASSERT_ND(level > 0);  // otherwise why came here?
      if (level == 0) {
        break;
      } else {
        continue;
      }
    }

    switched = true;
    // page switched! we have to allocate a new page and point to it.
    ArrayPage* parent = cur_path_[level];
    cur_route_.route[level] = next_route_.route[level];
    DualPagePointer& pointer = parent->get_interior_record(cur_route_.route[level]);
    ASSERT_ND(pointer.volatile_pointer_.word == 0);
    SnapshotPagePointer old_page_id = pointer.snapshot_pointer_;
    ASSERT_ND((previous_root_page_pointer_ != 0 && old_page_id != 0) ||
      (previous_root_page_pointer_ == 0 && old_page_id == 0));

    ArrayPage* page;
    if (level > 1) {
      // we switched an intermediate page
      page = intermediate_base_ + allocated_intermediates_;
      SnapshotPagePointer new_page_id = allocated_intermediates_;
      ++allocated_intermediates_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, level, cur_route_, page));
      pointer.volatile_pointer_.components.flags = kFlagIntermediatePointer;
      pointer.volatile_pointer_.components.offset = new_page_id;
    } else {
      ASSERT_ND(level == 1);
      // we switched a leaf page. in this case, we might have to flush the buffer
      if (allocated_pages_ >= max_pages_) {
        // dump everything in main buffer (intermediate pages are kept)
        CHECK_ERROR_CODE(snapshot_writer_->dump_pages(0, allocated_pages_));
        ASSERT_ND(snapshot_writer_->get_next_page_id()
          == page_base_[0].header().page_id_ + allocated_pages_);
        ASSERT_ND(snapshot_writer_->get_next_page_id()
          == page_base_[allocated_pages_ - 1].header().page_id_ + 1);
        allocated_pages_ = 0;
      }

      // remember, we can finalize the page ID of leaf pages at this point
      page = page_base_ + allocated_pages_;
      SnapshotPagePointer new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
      ++allocated_pages_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, level, cur_route_, page));
      pointer.snapshot_pointer_ = new_page_id;
    }
    cur_path_[level] = page;
  }
  return kErrorCodeOk;
}

inline ErrorCode ArrayComposer::read_or_init_page(
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

inline const ArrayOverwriteLogType* ArrayComposer::get_next_entry() const {
  ASSERT_ND(!inputs_[next_input_].ended_);
  const ArrayOverwriteLogType* entry = inputs_[next_input_].get_entry();
  ASSERT_ND(entry->offset_ == next_key_);
  ASSERT_ND(entry->header_.xct_id_ == next_xct_id_);
  return entry;
}

inline ArrayRange ArrayComposer::calculate_array_range(LookupRoute route, uint8_t level) const {
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
  return range;
}

void ArrayComposer::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<ArrayComposer>"
      << "<partitioner_>" << partitioner_ << "</partitioner_>"
      << "<snapshot_writer_>" << snapshot_writer_ << "</snapshot_writer_>"
      << "<new_snapshot>" << new_snapshot_ << "</new_snapshot>"
    << "</ArrayComposer>";
}

std::string ArrayComposer::to_string() const {
  return std::string("ArrayComposer:storage-") + std::to_string(partitioner_->get_storage_id())
    + std::string(":writer-") + snapshot_writer_->to_string();
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
