/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/log_reducer_impl.hpp"

#include <glog/logging.h>

#include <ostream>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/snapshot/log_gleaner_impl.hpp"

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
  return kRetOk;
}

ErrorStack LogReducer::handle_uninitialize() {
  ErrorStackBatch batch;
  buffer_memory_.release_block();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack LogReducer::handle_process() {
  SPINLOCK_WHILE(!parent_->is_all_mappers_completed()) {
    WRAP_ERROR_CODE(check_cancelled());
  }
  return kRetOk;
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


std::ostream& operator<<(std::ostream& o, const LogReducer& v) {
  o << "<LogReducer>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<thread_>" << v.thread_ << "</thread_>"
    << "</LogReducer>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
