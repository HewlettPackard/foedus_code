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
  : engine_(engine),
    partitioner_(partitioner),
    snapshot_writer_(snapshot_writer),
    previous_snapshot_files_(previous_snapshot_files),
    new_snapshot_(new_snapshot),
    storage_id_(partitioner->get_storage_id()),
    storage_(dynamic_cast<ArrayStorage*>(engine->get_storage_manager().get_storage(storage_id_))),
    previous_root_page_pointer_(storage_->get_metadata()->root_snapshot_page_id_),
    levels_(storage_->get_levels()),
    route_finder_(levels_, storage_->get_payload_size()) {
  ASSERT_ND(partitioner);
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
  WRAP_ERROR_CODE(compose_init_context(
    root_info_page_casted,
    work_memory,
    log_streams,
    log_streams_count));
  CHECK_ERROR(compose_strawman_tournament());

  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

ErrorStack ArrayComposer::compose_strawman_tournament() {
  while (inputs_count_ != ended_inputs_count_) {
    const ArrayOverwriteLogType* entry = get_next_entry();


    WRAP_ERROR_CODE(advance());
  }
  return kRetOk;
}

ErrorStack ArrayComposer::construct_root(
  const Page* const* /*root_info_pages*/,
  uint32_t /*root_info_pages_count*/,
  const memory::AlignedMemorySlice& /*work_memory*/,
  SnapshotPagePointer* new_root_page_pointer) {
  *new_root_page_pointer = 0;
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
  RootInfoPage* root_info_page,
  const memory::AlignedMemorySlice& work_memory,
  snapshot::SortedBuffer* const* inputs,
  uint32_t inputs_count) {
  root_info_page_ = root_info_page;

  char* work_buffer = reinterpret_cast<char*>(work_memory.get_block());
  uint64_t buffer_offset = 0;
  root_page_ = reinterpret_cast<ArrayPage*>(work_buffer + buffer_offset);
  buffer_offset += sizeof(ArrayPage);
  inputs_ = reinterpret_cast<StreamStatus*>(work_buffer + buffer_offset);
  buffer_offset += assorted::align<uint64_t, 4096>(sizeof(StreamStatus) * inputs_count);
  ASSERT_ND(work_memory.get_size() >= buffer_offset);
  // to get more aligned memories, add it here.

  inputs_count_ = inputs_count;
  ended_inputs_count_ = 0;
  for (uint32_t i = 0; i < inputs_count; ++i) {
    inputs_[i].init(inputs[i]);
  }

  std::memset(cur_path_, 0, sizeof(cur_path_));
  cur_route_.word = 0;
  cur_page_starts_ = 0;
  cur_page_ends_ = 0;
  std::memset(root_info_page_, 0, kPageSize);
  root_info_page_->header_.storage_id_ = storage_id_;

  // let's load the first pages. what's the first key/xct_id?
  next_input_ = inputs_count_;
  next_key_ = 0xFFFFFFFFFFFFFFFFULL;
  next_xct_id_ = xct::XctId(0xFFFFFFFFFFFFFFFFULL);
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

  // initialize cur_xxx with the first page.
  if (previous_root_page_pointer_ == 0) {
    // first snapshotting. So, no previous page image.
  } else {
    SnapshotPagePointer page_id = previous_root_page_pointer_;
    for (uint8_t level = levels_ - 1; level >= 0; --level) {
      ASSERT_ND(page_id > 0);
      memory::PagePoolOffset inmemory_offset;
      ArrayPage* page;
      if (level == levels_ - 1) {
        // root page is separated from the buffer in snapshot_writer
        // as we don't write it out here.
        inmemory_offset = 0;
        page = root_page_;
      } else {
        inmemory_offset = snapshot_writer_->allocate_new_page();
        page = reinterpret_cast<ArrayPage*>(snapshot_writer_->resolve(inmemory_offset));
      }
      cur_path_[level] = page;

      ASSERT_ND(page);
      CHECK_ERROR_CODE(previous_snapshot_files_->read_page(page_id, page));
      ASSERT_ND(page->header().storage_id_ == storage_id_);
      ASSERT_ND(page->header().page_id_ == page_id);
      ASSERT_ND(page->get_node_height() == level);
      if (level > 0) {
        page_id = page->get_interior_record(next_route_.route[level]).snapshot_pointer_;
      }
    }
  }
  return kErrorCodeOk;
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
  next_xct_id_ = xct::XctId(0xFFFFFFFFFFFFFFFFULL);
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
  update_next_route();
  return kErrorCodeOk;
}

inline void ArrayComposer::update_next_route() {
  if (next_key_ < next_page_ends_) {
    ASSERT_ND(next_key_ >= next_page_starts_);
    next_route_.route[0] = next_key_ - next_page_starts_;
    ASSERT_ND(next_route_.word == route_finder_.find_route(next_key_).word);
  } else {
    next_route_ = route_finder_.find_route_and_switch(
      next_key_,
      &next_page_starts_,
      &next_page_ends_);
  }
}

inline const ArrayOverwriteLogType* ArrayComposer::get_next_entry() const {
  ASSERT_ND(!inputs_[next_input_].ended_);
  const ArrayOverwriteLogType* entry = inputs_[next_input_].get_entry();
  ASSERT_ND(entry->offset_ == next_key_);
  ASSERT_ND(entry->header_.xct_id_ == next_xct_id_);
  return entry;
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
