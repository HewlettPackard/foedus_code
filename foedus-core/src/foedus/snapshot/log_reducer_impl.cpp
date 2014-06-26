/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/log_reducer_impl.hpp"

#include <glog/logging.h>

#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/snapshot/log_gleaner_impl.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/storage/partitioner.hpp"

namespace foedus {
namespace snapshot {

ErrorStack LogReducer::handle_initialize() {
  const SnapshotOptions& option = engine_->get_options().snapshot_;

  uint64_t buffer_size = static_cast<uint64_t>(option.log_reducer_buffer_mb_) << 20;
  buffer_memory_.alloc(
    buffer_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  uint64_t half_size = buffer_size >> 1;
  buffers_[0].buffer_slice_ = memory::AlignedMemorySlice(&buffer_memory_, 0, half_size);
  buffers_[0].status_.store(0);
  buffers_[1].buffer_slice_ = memory::AlignedMemorySlice(&buffer_memory_, half_size, half_size);
  buffers_[1].status_.store(0);

  // start from 1/16 of the main buffer. Should be big enough.
  sort_buffer_.alloc(
    buffer_size >> 4,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);

  // start from 1/16 of the main buffer. Should be big enough.
  positions_buffers_.alloc(
    buffer_size >> 4,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  input_positions_slice_ = memory::AlignedMemorySlice(
    &positions_buffers_,
    0,
    positions_buffers_.get_size() >> 1);
  output_positions_slice_ = memory::AlignedMemorySlice(
    &positions_buffers_,
    positions_buffers_.get_size() >> 1,
    positions_buffers_.get_size() >> 1);

  current_buffer_ = 0;
  sorted_runs_ = 0;
  return kRetOk;
}

ErrorStack LogReducer::handle_uninitialize() {
  ErrorStackBatch batch;
  buffer_memory_.release_block();
  sort_buffer_.release_block();
  positions_buffers_.release_block();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack LogReducer::handle_process() {
  SPINLOCK_WHILE(!parent_->is_all_mappers_completed()) {
    WRAP_ERROR_CODE(check_cancelled());
  }
  return kRetOk;
}

// void LogReducer::dump_buffer_sort_storage() {}
ErrorStack LogReducer::dump_buffer() {
  LOG(INFO) << "Sorting and dumping " << to_string() << "'s buffer to a file."
    << " current sorted_runs_=" << sorted_runs_;
  ReducerBuffer& buffer = buffers_[sorted_runs_ % 2];
  if (!buffer.is_no_more_writers()) {
    LOG(FATAL) << "wtf. this buffer is still open for writers";
  }

  dump_buffer_wait_for_writers(buffer);
  WRAP_ERROR_CODE(check_cancelled());

  BufferStatus final_status = buffer.get_status();
  debugging::StopWatch stop_watch;
  LOG(INFO) << to_string() << " Started sort/dump " <<
    from_buffer_position(final_status.components.tail_position_) << " bytes of logs";

  char* const base = reinterpret_cast<char*>(buffer.buffer_slice_.get_block());
  std::map<storage::StorageId, std::vector<BufferPosition> > blocks;
  dump_buffer_scan_block_headers(base, final_status.components.tail_position_, &blocks);

  // TODO(Hideaki) open a file
  for (auto& kv : blocks) {
    WRAP_ERROR_CODE(check_cancelled());
    storage::StorageId storage_id = kv.first;
    const std::vector<BufferPosition>& positions = kv.second;
    // first, count how many log entries are there. this is quick as we have a statistics
    // in the header.
    uint64_t records = 0;
    for (BufferPosition position : positions) {
      BlockHeader* header = reinterpret_cast<BlockHeader*>(base + from_buffer_position(position));
      if (header->magic_word_ != kBlockHeaderMagicWord) {
        LOG(FATAL) << to_string() << " wtf. magic word doesn't match. position=" << position
          << ", storage_id=" << storage_id;
      }
      records += header->log_count_;
    }

    // now we need a memory for this long array. expand the memory if not sufficient.
    uint64_t positions_buffer_size = records * sizeof(BufferPosition);
    expand_positions_buffers_if_needed(positions_buffer_size);
    BufferPosition* inputs = reinterpret_cast<BufferPosition*>(input_positions_slice_.get_block());
    uint64_t cur_rec_total = 0;

    // put all log positions to the array
    for (BufferPosition position : positions) {
      BlockHeader* header = reinterpret_cast<BlockHeader*>(base + from_buffer_position(position));
      if (header->magic_word_ != kBlockHeaderMagicWord) {
        LOG(FATAL) << to_string() << " wtf. magic word doesn't match. position=" << position
          << ", storage_id=" << storage_id;
      }
      uint64_t record_pos = from_buffer_position(position) + sizeof(BlockHeader);
      for (uint32_t i = 0; i < header->log_count_; ++i) {
        log::RecordLogType* record = reinterpret_cast<log::RecordLogType*>(base + record_pos);
        ASSERT_ND(record->header_.storage_id_ == storage_id);
        inputs[cur_rec_total] = to_buffer_position(record_pos);
        ++cur_rec_total;
        record_pos += record->header_.log_length_;
      }
      ASSERT_ND(record_pos == from_buffer_position(position + header->block_length_));
    }
    ASSERT_ND(cur_rec_total == records);

    // Now, sort these log records by key and then ordinal. we use the partitioner object for this.
    const storage::Partitioner* partitioner = parent_->get_or_create_partitioner(storage_id);
    ASSERT_ND(partitioner);
    expand_sort_buffer_if_needed(partitioner->get_required_sort_buffer_size(records));
    BufferPosition* outputs = reinterpret_cast<BufferPosition*>(
      output_positions_slice_.get_block());
    uint32_t written_count = 0;
    partitioner->sort_batch(
      LogBuffer(base),
      inputs,
      records,
      memory::AlignedMemorySlice(&sort_buffer_),
      parent_->get_snapshot()->base_epoch_,
      outputs,
      &written_count);

    // TODO(Hideaki) dump it to a file
  }

  stop_watch.stop();
  LOG(INFO) << to_string() << " Done sort/dump " <<
    from_buffer_position(final_status.components.tail_position_) << " bytes in "
    << stop_watch.elapsed_ms() << "ms";

  // clear the status so that mappers can start using this buffer.
  // note that this reducer has to do the buffer switch before mapper can really start using it.
  // this reducer immediately checks if it should do so right after this function.
  ++sorted_runs_;
  buffer.status_.store(0);
  return kRetOk;
}

ErrorStack LogReducer::dump_buffer_wait_for_writers(const ReducerBuffer& buffer) const {
  debugging::StopWatch wait_watch;
  SPINLOCK_WHILE(buffer.get_status().components.active_writers_ > 0) {
    WRAP_ERROR_CODE(check_cancelled());
  }
  wait_watch.stop();
  LOG(INFO) << to_string() << " Okay, now active_writers==0. waited/looped for "
    << wait_watch.elapsed_us() << "us";
  // I'd be very surprised if we were waiting for more than 1000us.
  return kRetOk;
}

void LogReducer::dump_buffer_scan_block_headers(
  char* buffer_base,
  BufferPosition tail_position,
  std::map<storage::StorageId, std::vector<BufferPosition> > *blocks) const {
  debugging::StopWatch header_watch;
  const uint64_t end = from_buffer_position(tail_position);
  uint64_t cur = 0;
  uint32_t total_blocks = 0;
  while (cur < end) {
    BlockHeader* header = reinterpret_cast<BlockHeader*>(buffer_base + cur);
    if (header->magic_word_ != kBlockHeaderMagicWord) {
      LOG(FATAL) << to_string() << " wtf. magic word doesn't match. cur=" << cur;
    }
    auto it = blocks->find(header->storage_id_);
    if (it != blocks->end()) {
      it->second.push_back(to_buffer_position(cur));
    } else {
      std::vector<BufferPosition> vec;
      vec.reserve(1 << 10);
      vec.push_back(to_buffer_position(cur));
      blocks->insert(std::pair< storage::StorageId, std::vector<BufferPosition> >(
        header->storage_id_, vec));
    }
    cur += from_buffer_position(header->block_length_);
    ++total_blocks;
  }
  ASSERT_ND(cur == end);
  header_watch.stop();
  LOG(INFO) << to_string() << " Scanned all blocks. There were " << total_blocks << " blocks"
    << ", " << blocks->size() << " distinct storages."
    << " scan elapsed time=" << header_watch.elapsed_us() << "us";
}

void LogReducer::append_log_chunk(
  storage::StorageId storage_id,
  const char* send_buffer,
  uint32_t log_count,
  uint64_t send_buffer_size) {
#ifndef NDEBUG
  DVLOG(1) << "Appending a block of " << send_buffer_size << " bytes (" << log_count
    << " entries) to " << to_string() << "'s buffer for storage-" << storage_id;
  debugging::StopWatch stop_watch;
#endif  // NDEBUG
  const uint64_t required_size = send_buffer_size + sizeof(BlockHeader);
  ReducerBuffer* buffer = nullptr;
  uint64_t begin_position = 0;
  while (true) {
    uint32_t buffer_index = current_buffer_.load();
    buffer = &buffers_[buffer_index % 2];

    // If even the current buffer is marked as no more writers, the reducer is getting behind.
    // Mappers have to wait, potentially for a long time. So, let's just sleep.
    BufferStatus cur_status = buffer->get_status();
    if (cur_status.components.flags_ & kFlagNoMoreWriters) {
      current_buffer_changed_.wait([this, buffer_index]{
        return current_buffer_.load() > buffer_index;
      });
      continue;
    }

    // the buffer is now full. let's mark this buffer full and
    // then wake up reducer to do switch.
    if (cur_status.components.tail_position_ + required_size > buffer->buffer_slice_.get_size()) {
      BufferStatus new_status = cur_status;
      new_status.components.flags_ |= kFlagNoMoreWriters;
      if (!buffer->status_.compare_exchange_strong(cur_status.word, new_status.word)) {
        // if CAS fails, someone else might have already done it. retry
        continue;
      }

      thread_.wakeup();
      continue;
    }

    // okay, "looks like" we can append our log. make it sure with atomic CAS
    BufferStatus new_status = cur_status;
    ++new_status.components.active_writers_;
    new_status.components.tail_position_ += to_buffer_position(required_size);
    if (!buffer->status_.compare_exchange_strong(cur_status.word, new_status.word)) {
      // someone else did something. retry
      continue;
    }

    // okay, we atomically reserved the space.
    begin_position = from_buffer_position(cur_status.components.tail_position_);
    break;
  }

  ASSERT_ND(buffer);

  // now start copying. this might take a few tens of microseconds if it's 1MB and on another
  // NUMA node.
#ifndef NDEBUG
  debugging::StopWatch copy_watch;
#endif  // NDEBUG
  char* destination = reinterpret_cast<char*>(buffer->buffer_slice_.get_block()) + begin_position;
  BlockHeader header;
  header.storage_id_ = storage_id;
  header.log_count_ = log_count;
  header.block_length_ = to_buffer_position(required_size);
  header.magic_word_ = kBlockHeaderMagicWord;
  std::memcpy(destination, &header, sizeof(BlockHeader));
  std::memcpy(destination + sizeof(BlockHeader), send_buffer, send_buffer_size);
#ifndef NDEBUG
  copy_watch.stop();
  DVLOG(1) << "memcpy of " << send_buffer_size << " bytes took " << copy_watch.elapsed_ns() << "ns";
#endif  // NDEBUG

  // done, let's decrement the active_writers_ to declare we are done.
  while (true) {
    BufferStatus cur_status = buffer->get_status();
    BufferStatus new_status = cur_status;
    ASSERT_ND(new_status.components.active_writers_ > 0);
    --new_status.components.active_writers_;
    if (!buffer->status_.compare_exchange_strong(cur_status.word, new_status.word)) {
      // if CAS fails, someone else might have already done it. retry
      continue;
    }

    // okay, decremented. let's exit.

    // Disabled. for now the reducer does spin. so no need for wakeup
    // if (new_status.components.active_writers_ == 0
    //   && (new_status.components.flags_ & kFlagNoMoreWriters)) {
    //   // if this was the last writer and the buffer was already closed for new writers,
    //   // the reducer might be waiting for us. let's wake her up
    //   thread_.wakeup();
    // }
    break;
  }

#ifndef NDEBUG
  stop_watch.stop();
  DVLOG(1) << "Completed appending a block of " << send_buffer_size << " bytes to " << to_string()
    << "'s buffer for storage-" << storage_id << " in " << stop_watch.elapsed_ns() << "ns";
#endif  // NDEBUG
}

void LogReducer::expand_sort_buffer_if_needed(uint64_t required_size) {
  if (sort_buffer_.get_size() < required_size) {
    LOG(WARNING) << to_string() << " automatically expanding sort_buffer from "
      << sort_buffer_.get_size() << " to " << required_size << ". if this happens often,"
      << " our sizing is wrong.";
      sort_buffer_.alloc(
        required_size,
        memory::kHugepageSize,
        memory::AlignedMemory::kNumaAllocOnnode,
        numa_node_);
  }
}

void LogReducer::expand_positions_buffers_if_needed(uint64_t required_size_per_buffer) {
  ASSERT_ND(input_positions_slice_.get_size() == output_positions_slice_.get_size());
  if (input_positions_slice_.get_size() < required_size_per_buffer) {
    uint64_t new_size = required_size_per_buffer * 2;
    LOG(WARNING) << to_string() << " automatically expanding positions_buffers from "
      << positions_buffers_.get_size() << " to " << new_size << ". if this happens often,"
      << " our sizing is wrong.";
      positions_buffers_.alloc(
        new_size,
        memory::kHugepageSize,
        memory::AlignedMemory::kNumaAllocOnnode,
        numa_node_);
      input_positions_slice_ = memory::AlignedMemorySlice(
        &positions_buffers_,
        0,
        positions_buffers_.get_size() >> 1);
      output_positions_slice_ = memory::AlignedMemorySlice(
        &positions_buffers_,
        positions_buffers_.get_size() >> 1,
        positions_buffers_.get_size() >> 1);
  }
}

std::ostream& operator<<(std::ostream& o, const LogReducer& v) {
  o << "<LogReducer>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<buffer_memory_>" << v.buffer_memory_ << "</buffer_memory_>"
    << "<sort_buffer_>" << v.sort_buffer_ << "</sort_buffer_>"
    << "<positions_buffers_>" << v.positions_buffers_ << "</positions_buffers_>"
    << "<current_buffer_>" << v.current_buffer_ << "</current_buffer_>"
    << "<sorted_runs_>" << v.sorted_runs_ << "</sorted_runs_>"
    << "<thread_>" << v.thread_ << "</thread_>"
    << "</LogReducer>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
