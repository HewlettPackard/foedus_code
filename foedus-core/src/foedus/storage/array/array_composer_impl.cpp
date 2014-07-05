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
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
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
    const snapshot::Snapshot& new_snapshot)
  : engine_(engine),
    partitioner_(partitioner),
    snapshot_writer_(snapshot_writer),
    new_snapshot_(new_snapshot) {
  ASSERT_ND(partitioner);
}
struct StreamStatus {
  void init(snapshot::SortedBuffer* stream) {
    stream_ = stream;
    buffer_ = stream->get_buffer();
    buffer_size_ = stream->get_buffer_size();
    cur_absolute_pos_ = stream->get_cur_block_abosulte_begin();
    cur_relative_pos_ = stream->get_offset();
    end_absolute_pos_ = stream->get_cur_block_abosulte_end();
    ended_ = false;
    read_entry();
  }
  ErrorCode next() {
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
  void read_entry() {
    const ArrayOverwriteLogType* entry = get_entry();
    ASSERT_ND(entry->header_.get_type() == log::kLogCodeArrayOverwrite);
    ASSERT_ND(entry->header_.log_length_ > 0);
    cur_value_ = entry->offset_;
    cur_xct_id_ = entry->header_.xct_id_;
    cur_length_ = entry->header_.log_length_;
  }
  const ArrayOverwriteLogType* get_entry() const {
    return reinterpret_cast<const ArrayOverwriteLogType*>(buffer_ + cur_relative_pos_);
  }

  snapshot::SortedBuffer* stream_;
  const char*     buffer_;
  uint64_t        buffer_size_;
  uint64_t        cur_absolute_pos_;
  uint64_t        cur_relative_pos_;
  uint64_t        end_absolute_pos_;
  ArrayOffset     cur_value_;
  xct::XctId      cur_xct_id_;
  uint32_t        cur_length_;
  bool            ended_;
};

ErrorStack ArrayComposer::strawman_tournament(
  snapshot::SortedBuffer** log_streams,
  uint32_t log_streams_count,
  SnapshotPagePointer previous_root_page_pointer,
  const memory::AlignedMemorySlice& work_memory) {
  // TODO(Hideaki): winner/loser tree to speed this up. binary or n-ary?
  // Or, batched sort with marking end of each buffer.
  // Also, special case for log_streams_count==1 case, which can happen.
  StreamStatus* status = reinterpret_cast<StreamStatus*>(work_memory.get_block());
  for (uint32_t i = 0; i < log_streams_count; ++i) {
    status[i].init(log_streams[i]);
  }

  // current_pages[0]=root, current_pages[levels - 1]=leaf
  ArrayPage* current_pages[8];  // array is surely within 8 levels.
  std::memset(current_pages, 0, sizeof(ArrayPage*) * 8);

  const uint8_t levels = partitioner_->get_array_levels();
  VLOG(0) << to_string() << ", prev root=" << previous_root_page_pointer
    << ", levels=" << levels;
  {
    // let's load the first pages. what's the first key?
    ArrayOffset smallest_value = 0xFFFFFFFFFFFFFFFFULL;
    for (uint32_t i = 0; i < log_streams_count; ++i) {
      if (status[i].cur_value_ < smallest_value) {
        smallest_value = status[i].cur_value_;
      }
    }
  }


  uint32_t ended_streams = 0;
  while (true) {
    uint32_t smallest_stream = log_streams_count;
    ArrayOffset smallest_value = 0xFFFFFFFFFFFFFFFFULL;
    xct::XctId  smallest_xct_id(0xFFFFFFFFFFFFFFFFULL);
    for (uint32_t i = 0; i < log_streams_count; ++i) {
      // TODO(Hideaki): rather than checking ended each time, we should re-allocate the array when
      // some stream is ended.
      if (!status[i].ended_) {
        if (status[i].cur_value_ < smallest_value || (
            // "ordinal ==" is not an issue. If there is a conflict, we did issue a new ordinal.
            // so, it can't happen.
            status[i].cur_value_ == smallest_value &&
            status[i].cur_xct_id_.before(smallest_xct_id))) {
          smallest_stream = i;
          smallest_value = status[i].cur_value_;
          smallest_xct_id = status[i].cur_xct_id_;
        }
      }
    }
    ASSERT_ND(smallest_stream < log_streams_count);
    const ArrayOverwriteLogType* entry = status[smallest_stream].get_entry();
    ASSERT_ND(entry->offset_ == smallest_value);
    ASSERT_ND(entry->header_.xct_id_.equals_all(smallest_xct_id));
    // TODO(Hideaki): use it

    // then, read next
    WRAP_ERROR_CODE(status[smallest_stream].next());
    if (status[smallest_stream].ended_) {
      ++ended_streams;
      if (ended_streams >= log_streams_count) {
        break;
      }
    }
  }
  return kRetOk;
}

ErrorStack ArrayComposer::compose(
  snapshot::SortedBuffer** log_streams,
  uint32_t log_streams_count,
  SnapshotPagePointer previous_root_page_pointer,
  const memory::AlignedMemorySlice& work_memory,
  Page* /*root_info_page*/) {
  VLOG(0) << to_string() << " composing with " << log_streams_count << " streams."
    << " previous_root_page_pointer=" << assorted::Hex(previous_root_page_pointer);
  debugging::StopWatch stop_watch;

  CHECK_ERROR(strawman_tournament(
    log_streams,
    log_streams_count,
    previous_root_page_pointer,
    work_memory));

  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

uint64_t ArrayComposer::get_required_work_memory_size(
  snapshot::SortedBuffer** /*log_streams*/,
  uint32_t log_streams_count) const {
  return sizeof(StreamStatus) * log_streams_count;
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
