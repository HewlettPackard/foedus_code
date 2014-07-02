/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_composer_impl.hpp"

#include <glog/logging.h>

#include <ostream>
#include <string>

#include "foedus/debugging/stop_watch.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"

namespace foedus {
namespace storage {
namespace sequential {

SequentialComposer::SequentialComposer(
    Engine *engine,
    const SequentialPartitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    const snapshot::Snapshot& new_snapshot)
  : engine_(engine),
    partitioner_(partitioner),
    snapshot_writer_(snapshot_writer),
    new_snapshot_(new_snapshot) {
  ASSERT_ND(partitioner);
}

/**
 * @todo some of below should become a SortedBuffer method.
 */
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
    const SequentialAppendLogType* entry = get_entry();
    ASSERT_ND(entry->header_.get_type() == log::kLogCodeSequentialAppend);
    ASSERT_ND(entry->header_.log_length_ > 0);
    cur_length_ = entry->header_.log_length_;
  }
  const SequentialAppendLogType* get_entry() const {
    return reinterpret_cast<const SequentialAppendLogType*>(buffer_ + cur_relative_pos_);
  }

  snapshot::SortedBuffer* stream_;
  const char*     buffer_;
  uint64_t        buffer_size_;
  uint64_t        cur_absolute_pos_;
  uint64_t        cur_relative_pos_;
  uint64_t        end_absolute_pos_;
  uint32_t        cur_length_;
  bool            ended_;
};

ErrorStack SequentialComposer::compose(
  snapshot::SortedBuffer** log_streams,
  uint32_t log_streams_count,
  SnapshotPagePointer previous_root_page_pointer,
  const memory::AlignedMemorySlice& /*work_memory*/) {
  VLOG(0) << to_string() << " composing with " << log_streams_count << " streams."
    << " previous_root_page_pointer=" << assorted::Hex(previous_root_page_pointer);
  debugging::StopWatch stop_watch;

  memory::PagePoolOffset current_page_offset = snapshot_writer_->allocate_new_page();
  SequentialPage* current_page = reinterpret_cast<SequentialPage*>(
    snapshot_writer_->resolve(current_page_offset));
  ASSERT_ND(current_page);
  for (uint32_t i = 0; i < log_streams_count; ++i) {
    StreamStatus status;
    status.init(log_streams[i]);
    const SequentialAppendLogType* entry = status.get_entry();

    if (current_page->get_used_data_bytes()
        + assorted::align8(entry->payload_count_) + kRecordOverhead > SequentialPage::kDataSize ||
        current_page->get_record_count() >= SequentialPage::kMaxSlots) {
      // now need to allocate a new page
      if (snapshot_writer_->is_full()) {
        // TODO(Hideaki) fix and dump
      }
      memory::PagePoolOffset new_page_offset = snapshot_writer_->allocate_new_page();
      SequentialPage* new_page = reinterpret_cast<SequentialPage*>(
        snapshot_writer_->resolve(new_page_offset));
      DualPagePointer new_page_pointer;
      new_page_pointer.volatile_pointer_.components.offset = new_page_offset;
      current_page->set_next_page(new_page_pointer);
      current_page_offset = new_page_offset;
      current_page = new_page;
    }

    ASSERT_ND(current_page->get_used_data_bytes() + status.cur_length_
      <= SequentialPage::kDataSize);
    ASSERT_ND(current_page->get_record_count() < SequentialPage::kMaxSlots);
    // TODO(Hideaki) put record in page, no need for atomics

    // then, read next
    WRAP_ERROR_CODE(status.next());
    if (status.ended_) {
      break;
    }
  }
  // TODO(Hideaki) fix and dump

  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

void SequentialComposer::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<SequentialComposer>"
      << "<partitioner_>" << partitioner_ << "</partitioner_>"
      << "<snapshot_writer_>" << snapshot_writer_ << "</snapshot_writer_>"
      << "<new_snapshot>" << new_snapshot_ << "</new_snapshot>"
    << "</SequentialComposer>";
}

std::string SequentialComposer::to_string() const {
  return std::string("SequentialComposer:storage-")
    + std::to_string(partitioner_->get_storage_id());
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
