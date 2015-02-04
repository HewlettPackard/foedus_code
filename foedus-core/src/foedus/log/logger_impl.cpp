/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/log/logger_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_group.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace log {

inline bool is_log_aligned(uint64_t offset) {
  return offset % FillerLogType::kLogWriteUnitSize == 0;
}
inline uint64_t align_log_ceil(uint64_t offset) {
  return assorted::align< uint64_t, FillerLogType::kLogWriteUnitSize >(offset);
}
inline uint64_t align_log_floor(uint64_t offset) {
  if (offset % FillerLogType::kLogWriteUnitSize == 0) {
    return offset;
  } else {
    return assorted::align< uint64_t, FillerLogType::kLogWriteUnitSize >(offset)
      - FillerLogType::kLogWriteUnitSize;
  }
}

ErrorStack Logger::initialize_once() {
  control_block_->initialize();
  // clear all variables
  current_file_ = nullptr;
  LOG(INFO) << "Initializing Logger-" << id_ << ". assigned " << assigned_thread_ids_.size()
    << " threads, starting from " << assigned_thread_ids_[0] << ", numa_node_="
    << static_cast<int>(numa_node_);

  // Initialize the values from the latest savepoint.
  savepoint::LoggerSavepointInfo info = engine_->get_savepoint_manager()->get_logger_savepoint(id_);
  // durable epoch from initial savepoint
  control_block_->durable_epoch_
    = engine_->get_savepoint_manager()->get_initial_durable_epoch().value();
  control_block_->marked_epoch_ = Epoch(control_block_->durable_epoch_);
  control_block_->oldest_ordinal_ = info.oldest_log_file_;  // ordinal/length too
  control_block_->current_ordinal_ = info.current_log_file_;
  control_block_->current_file_durable_offset_ = info.current_log_file_offset_durable_;
  control_block_->oldest_file_offset_begin_ = info.oldest_log_file_offset_begin_;
  current_file_path_ = engine_->get_options().log_.construct_suffixed_log_path(
    numa_node_,
    id_,
    control_block_->current_ordinal_);
  // open the log file
  current_file_ = new fs::DirectIoFile(current_file_path_,
                     engine_->get_options().log_.emulation_);
  WRAP_ERROR_CODE(current_file_->open(true, true, true, true));
  if (control_block_->current_file_durable_offset_ < current_file_->get_current_offset()) {
    // there are non-durable regions as an incomplete remnant of previous execution.
    // probably there was a crash. in this case, we discard the non-durable regions.
    LOG(ERROR) << "Logger-" << id_ << "'s log file has a non-durable region. Probably there"
      << " was a crash. Will truncate it to " << control_block_->current_file_durable_offset_
      << " from " << current_file_->get_current_offset();
    WRAP_ERROR_CODE(current_file_->truncate(
      control_block_->current_file_durable_offset_,
      true));  // sync right now
  }
  ASSERT_ND(control_block_->current_file_durable_offset_ == current_file_->get_current_offset());
  LOG(INFO) << "Initialized logger: " << *this;

  // which threads are assigned to me?
  for (auto thread_id : assigned_thread_ids_) {
    assigned_threads_.push_back(
      engine_->get_thread_pool()->get_pimpl()->get_local_group()->get_thread(
        thread::decompose_numa_local_ordinal(thread_id)));
  }

  // grab a buffer to pad incomplete blocks for direct file I/O
  CHECK_ERROR(engine_->get_memory_manager()->get_local_memory()->allocate_numa_memory(
    FillerLogType::kLogWriteUnitSize, &fill_buffer_));
  ASSERT_ND(!fill_buffer_.is_null());
  ASSERT_ND(fill_buffer_.get_size() >= FillerLogType::kLogWriteUnitSize);
  ASSERT_ND(fill_buffer_.get_alignment() >= FillerLogType::kLogWriteUnitSize);
  LOG(INFO) << "Logger-" << id_ << " grabbed a padding buffer. size=" << fill_buffer_.get_size();
  CHECK_ERROR(write_dummy_epoch_mark());

  // log file and buffer prepared. let's launch the logger thread
  logger_thread_ = std::move(std::thread(&Logger::handle_logger, this));

  assert_consistent();
  return kRetOk;
}

ErrorStack Logger::uninitialize_once() {
  LOG(INFO) << "Uninitializing Logger-" << id_ << ": " << *this;
  ErrorStackBatch batch;
  if (logger_thread_.joinable()) {
    {
      control_block_->stop_requested_ = true;
      control_block_->wakeup_cond_.signal();
    }
    logger_thread_.join();
  }
  if (current_file_) {
    current_file_->close();
    delete current_file_;
    current_file_ = nullptr;
  }
  fill_buffer_.release_block();
  control_block_->uninitialize();
  return SUMMARIZE_ERROR_BATCH(batch);
}


void Logger::handle_logger() {
  LOG(INFO) << "Logger-" << id_ << " started. pin on NUMA node-" << static_cast<int>(numa_node_);
  thread::NumaThreadScope scope(numa_node_);
  // The actual logging can't start until XctManager is initialized.
  SPINLOCK_WHILE(!is_stop_requested() && !engine_->get_xct_manager()->is_initialized()) {
    assorted::memory_fence_acquire();
  }

  LOG(INFO) << "Logger-" << id_ << " now starts logging";
  while (!is_stop_requested()) {
    {
      uint64_t demand = control_block_->wakeup_cond_.acquire_ticket();
      if (!is_stop_requested()) {
        control_block_->wakeup_cond_.timedwait(demand, 10000ULL);
      }
    }
    const int kMaxIterations = 100;
    int iterations = 0;

    while (!is_stop_requested()) {
      assert_consistent();
      Epoch current_epoch = engine_->get_xct_manager()->get_current_global_epoch();
      Epoch durable_epoch = get_durable_epoch();
      ASSERT_ND(durable_epoch < current_epoch);
      Epoch next_durable = durable_epoch.one_more();
      if (next_durable == current_epoch.one_less()) {
        DVLOG(2) << "Logger-" << id_ << " is well catching up. will sleep.";
        break;
      }

      // just for debug out
      debugging::StopWatch watch;
      uint64_t before_offset = (current_file_ ? current_file_-> get_current_offset() : 0);

      COERCE_ERROR(write_one_epoch(next_durable));
      ASSERT_ND(get_durable_epoch() == next_durable);
      COERCE_ERROR(switch_file_if_required());

      watch.stop();
      uint64_t after_offset = (current_file_ ? current_file_-> get_current_offset() : 0);
      // LOG(INFO) was too noisy
      if (after_offset != before_offset) {
        VLOG(0) << "Logger-" << id_ << " wrote out " << (after_offset - before_offset)
          << " bytes for epoch-" << next_durable << " in " << watch.elapsed_ms() << " ms";
      }

      if (((++iterations) % kMaxIterations) == 0) {
        LOG(WARNING) << "Logger-" << id_ << " has been working without sleep for long time"
          << "(" << iterations << "). Either too few loggers or potentially a bug?? "
          << *this;
      } else {
        VLOG(0) << "Logger-" << id_ << " has more task. keep working. " << iterations;
        DVLOG(1) << *this;
      }
    }
  }
  LOG(INFO) << "Logger-" << id_ << " ended. " << *this;
}

ErrorStack Logger::update_durable_epoch(Epoch new_durable_epoch, bool had_any_log) {
  DVLOG(1) << "Checked all loggers. new_durable_epoch=" << new_durable_epoch;
  if (had_any_log) {
    VLOG(0) << "Logger-" << id_ << " updating durable_epoch_ from " << get_durable_epoch()
      << " to " << new_durable_epoch;

    // BEFORE updating the epoch, fsync the file AND the parent folder
    if (!fs::fsync(current_file_path_, true)) {
      return ERROR_STACK_MSG(kErrorCodeFsSyncFailed, to_string().c_str());
    }
    control_block_->current_file_durable_offset_ = current_file_->get_current_offset();
    VLOG(0) << "Logger-" << id_ << " fsynced the current file ("
      << control_block_->current_file_durable_offset_ << "  bytes so far) and its folder";
    DVLOG(0) << "Before: " << *this;
    assorted::memory_fence_release();  // announce it only AFTER above
  } else {
    VLOG(0) << "Logger-" << id_ << " had no log in this epoch. not writing an epoch mark."
      << " durable ep=" << get_durable_epoch() << ", new_epoch=" << new_durable_epoch
      << " marked ep=" << control_block_->marked_epoch_;
    ASSERT_ND(control_block_->current_file_durable_offset_ == current_file_->get_current_offset());
  }

  ASSERT_ND(new_durable_epoch >= Epoch(control_block_->durable_epoch_));
  if (new_durable_epoch > Epoch(control_block_->durable_epoch_)) {
    // This must be the only place to set durable_epoch_
    control_block_->durable_epoch_ = new_durable_epoch.value();

    // finally, let the log manager re-calculate the global durable epoch.
    // this may or may not result in new global durable epoch
    assorted::memory_fence_release();
    CHECK_ERROR(engine_->get_log_manager()->refresh_global_durable_epoch());

    if (had_any_log) {
      DVLOG(0) << "After: " << *this;
    }
  }

  assert_consistent();
  return kRetOk;
}
ErrorStack Logger::write_dummy_epoch_mark() {
  CHECK_ERROR(log_epoch_switch(get_durable_epoch()));
  LOG(INFO) << "Logger-" << id_ << " wrote out a dummy epoch marker at the beginning";
  CHECK_ERROR(update_durable_epoch(get_durable_epoch(), true));  // flush the epoch mark immediately
  return kRetOk;
}

ErrorStack Logger::log_epoch_switch(Epoch new_epoch) {
  ASSERT_ND(control_block_->marked_epoch_ <= new_epoch);
  VLOG(0) << "Writing epoch marker for Logger-" << id_
    << ". marked_epoch_=" << control_block_->marked_epoch_ << " new_epoch=" << new_epoch;
  DVLOG(1) << *this;

  // Use fill buffer to write out the epoch mark log
  std::lock_guard<std::mutex> guard(epoch_switch_mutex_);
  char* buf = reinterpret_cast<char*>(fill_buffer_.get_block());
  EpochMarkerLogType* epoch_marker = reinterpret_cast<EpochMarkerLogType*>(buf);
  epoch_marker->populate(
    control_block_->marked_epoch_,
    new_epoch,
    numa_node_,
    in_node_ordinal_,
    id_,
    control_block_->current_ordinal_,
    current_file_->get_current_offset());

  // Fill it up to 4kb and write. A bit wasteful, but happens only once per epoch
  FillerLogType* filler_log = reinterpret_cast<FillerLogType*>(buf
    + sizeof(EpochMarkerLogType));
  filler_log->populate(fill_buffer_.get_size() - sizeof(EpochMarkerLogType));

  WRAP_ERROR_CODE(current_file_->write(fill_buffer_.get_size(), fill_buffer_));
  control_block_->marked_epoch_ = new_epoch;
  add_epoch_history(*epoch_marker);

  assert_consistent();
  return kRetOk;
}

ErrorStack Logger::switch_file_if_required() {
  ASSERT_ND(current_file_);
  if (current_file_->get_current_offset()
      < (static_cast<uint64_t>(engine_->get_options().log_.log_file_size_mb_) << 20)) {
    return kRetOk;
  }

  LOG(INFO) << "Logger-" << id_ << " moving on to next file. " << *this;

  // Close the current one. Immediately call fsync on it AND the parent folder.
  current_file_->close();
  delete current_file_;
  current_file_ = nullptr;
  control_block_->current_file_durable_offset_ = 0;
  if (!fs::fsync(current_file_path_, true)) {
    return ERROR_STACK_MSG(kErrorCodeFsSyncFailed, to_string().c_str());
  }

  current_file_path_ = engine_->get_options().log_.construct_suffixed_log_path(
    numa_node_,
    id_,
    ++control_block_->current_ordinal_);
  LOG(INFO) << "Logger-" << id_ << " next file=" << current_file_path_;
  current_file_ = new fs::DirectIoFile(current_file_path_,
                      engine_->get_options().log_.emulation_);
  WRAP_ERROR_CODE(current_file_->open(true, true, true, true));
  ASSERT_ND(current_file_->get_current_offset() == 0);
  LOG(INFO) << "Logger-" << id_ << " moved on to next file. " << *this;
  CHECK_ERROR(write_dummy_epoch_mark());
  return kRetOk;
}

ErrorStack Logger::write_one_epoch(Epoch write_epoch) {
  ASSERT_ND(get_durable_epoch().one_more() == write_epoch);
  ASSERT_ND(write_epoch.one_more() < engine_->get_xct_manager()->get_current_global_epoch());
  bool had_any_log = false;
  for (thread::Thread* the_thread : assigned_threads_) {
    ThreadLogBuffer& buffer = the_thread->get_thread_log_buffer();
    ThreadLogBuffer::OffsetRange range = buffer.get_logs_to_write(write_epoch);
    ASSERT_ND(range.begin_ <= buffer.get_meta().buffer_size_);
    ASSERT_ND(range.end_ <= buffer.get_meta().buffer_size_);
    if (range.begin_ > buffer.get_meta().buffer_size_
      || range.end_ > buffer.get_meta().buffer_size_) {
      range = buffer.get_logs_to_write(write_epoch);
      LOG(FATAL) << "Logger-" << id_ << " reported an invalid buffer range for epoch-"
        << write_epoch << ". begin=" << range.begin_ << ", end=" << range.end_
          << " while log buffer size=" << buffer.get_meta().buffer_size_
          << ". " << *this;
    }

    if (!range.is_empty()) {
      if (had_any_log == false) {
        // First log for this epoch. Now we write out an epoch mark.
        // If no buffers have any logs, we don't even bother writing out an epoch mark.
        VLOG(1) << "Logger-" << id_ << " has a non-empty epoch-" << write_epoch;
        had_any_log = true;
        CHECK_ERROR(log_epoch_switch(write_epoch));
      }

      if (range.begin_ < range.end_) {
        CHECK_ERROR(write_one_epoch_piece(buffer, write_epoch, range.begin_, range.end_));
      } else {
        // oh, it wraps around.
        // let's write up to the end of the circular buffer, then from the beginning.
        // we can simply write out logs upto the end without worrying about the case where a log
        // entry spans the end of circular buffer. Because we avoid that in ThreadLogBuffer.
        // (see reserve_new_log()). So, we can separately handle the two writes by calling itself
        // again, which adds padding if they need.
        VLOG(0) << "Wraps around. from_offset=" << range.begin_ << ", upto_offset=" << range.end_;
        uint64_t capacity = buffer.get_meta().buffer_size_;
        CHECK_ERROR(write_one_epoch_piece(buffer, write_epoch, range.begin_, capacity));
        CHECK_ERROR(write_one_epoch_piece(buffer, write_epoch, 0, range.end_));
      }
    }
    buffer.on_log_written(write_epoch);
  }
  CHECK_ERROR(update_durable_epoch(write_epoch, had_any_log));
  return kRetOk;
}

ErrorStack Logger::write_one_epoch_piece(
  const ThreadLogBuffer& buffer,
  Epoch write_epoch,
  uint64_t from_offset,
  uint64_t upto_offset) {
  assert_consistent();
  ASSERT_ND(from_offset <= upto_offset);
  if (from_offset == upto_offset) {
    return kRetOk;
  }

  VLOG(0) << "Writing out Thread-" << buffer.get_thread_id() << "'s log. from_offset="
    << from_offset << ", upto_offset=" << upto_offset << ", write_epoch=" << write_epoch;
  DVLOG(1) << *this;

  const char* raw_buffer = buffer.get_buffer();
  assert_written_logs(write_epoch, raw_buffer + from_offset, upto_offset - from_offset);

  // 1) First-4kb. Do we have to pad at the beginning?
  if (!is_log_aligned(from_offset)) {
    VLOG(1) << "padding at beginning needed. ";
    char* buf = reinterpret_cast<char*>(fill_buffer_.get_block());

    // pad upto from_offset
    uint64_t begin_fill_size = from_offset - align_log_floor(from_offset);
    ASSERT_ND(begin_fill_size < FillerLogType::kLogWriteUnitSize);
    FillerLogType* begin_filler_log = reinterpret_cast<FillerLogType*>(buf);
    begin_filler_log->populate(begin_fill_size);
    buf += begin_fill_size;

    // then copy the log content, upto at most one page... is it one page? or less?
    uint64_t copy_size;
    if (upto_offset <= align_log_ceil(from_offset)) {
      VLOG(1) << "whole log in less than one page.";
      copy_size = upto_offset - from_offset;
    } else {
      VLOG(1) << "one page or more.";
      copy_size = align_log_ceil(from_offset) - from_offset;
    }
    ASSERT_ND(copy_size < FillerLogType::kLogWriteUnitSize);
    std::memcpy(buf, raw_buffer + from_offset, copy_size);
    buf += copy_size;

    // pad at the end, if needed
    uint64_t end_fill_size = FillerLogType::kLogWriteUnitSize - (begin_fill_size + copy_size);
    ASSERT_ND(end_fill_size < FillerLogType::kLogWriteUnitSize);
    // logs are all 8-byte aligned.
    // note that FillerLogType (16 bytes) is fully populated. We use only the first 8 bytes of it.
    ASSERT_ND(end_fill_size % 8 == 0);
    if (end_fill_size > 0) {
      FillerLogType* end_filler_log = reinterpret_cast<FillerLogType*>(buf);
      end_filler_log->populate(end_fill_size);
    }
    WRAP_ERROR_CODE(current_file_->write(FillerLogType::kLogWriteUnitSize, fill_buffer_));
    from_offset += copy_size;
  }

  if (from_offset >= upto_offset) {  // if it's larger, the entire logs were less than one page.
    return kRetOk;
  }

  // from here, "from" is assured to be aligned
  ASSERT_ND(is_log_aligned(from_offset));


  // 2) Middle regions where everything is aligned. easy
  uint64_t middle_size = align_log_floor(upto_offset) - from_offset;
  if (middle_size > 0) {
    // debugging::StopWatch watch;
    VLOG(1) << "Writing middle regions: " << middle_size << " bytes from " << from_offset;
    WRAP_ERROR_CODE(current_file_->write_raw(middle_size, raw_buffer + from_offset));
    // watch.stop();
    // mm, in fact too noisy... Maybe VLOG(0). but we need this information for the paper
    // LOG(INFO) << "Wrote middle regions of " << middle_size << " bytes in "
    //   << watch.elapsed_ms() << "ms";
    from_offset += middle_size;
  }

  ASSERT_ND(is_log_aligned(from_offset));
  ASSERT_ND(from_offset <= upto_offset);
  if (from_offset == upto_offset) {
    return kRetOk;  // if upto_offset is luckily aligned, we exit here.
  }

  ASSERT_ND(from_offset < upto_offset);
  ASSERT_ND(!is_log_aligned(upto_offset));

  // 3) the last 4kb
  VLOG(1) << "padding at end needed.";
  char* buf = reinterpret_cast<char*>(fill_buffer_.get_block());

  uint64_t copy_size = upto_offset - from_offset;
  ASSERT_ND(copy_size < FillerLogType::kLogWriteUnitSize);
  std::memcpy(buf, raw_buffer + from_offset, copy_size);
  buf += copy_size;

  // pad upto from_offset
  const uint64_t fill_size = FillerLogType::kLogWriteUnitSize - copy_size;
  FillerLogType* filler_log = reinterpret_cast<FillerLogType*>(buf);
  filler_log->populate(fill_size);

  WRAP_ERROR_CODE(current_file_->write(FillerLogType::kLogWriteUnitSize, fill_buffer_));
  return kRetOk;
}

void Logger::assert_written_logs(Epoch write_epoch, const char* logs, uint64_t bytes) const {
  ASSERT_ND(write_epoch.is_valid());
  ASSERT_ND(logs);
  ASSERT_ND(bytes);
#ifndef NDEBUG
  // all logs should be in this epoch. we can do this kind of sanity check thanks to epoch marker.
  assorted::memory_fence_acquire();
  uint64_t cur;
  storage::StorageId largest_storage_id = engine_->get_storage_manager()->get_largest_storage_id();
  uint32_t previous_ordinal = 0;
  for (cur = 0; cur < bytes;) {
    const LogHeader* header= reinterpret_cast<const LogHeader*>(logs + cur);
    cur += header->log_length_;
    ASSERT_ND(header->log_length_ > 0);
    ASSERT_ND(header->get_type() != kLogCodeEpochMarker);
    if (header->get_type() == log::kLogCodeFiller) {
      DLOG(INFO) << "Found a filler log in assert_written_logs: size=" << header->log_length_;
      continue;
    }
    // These are logs from individual threads, so must be non-meta, non-marker logs.
    ASSERT_ND(header->get_kind() == log::kRecordLogs);
    reinterpret_cast<const RecordLogType*>(header)->assert_valid_generic();
    ASSERT_ND(header->storage_id_ <= largest_storage_id);
    Epoch record_epoch = header->xct_id_.get_epoch();
    ASSERT_ND(record_epoch.is_valid());
    ASSERT_ND(write_epoch == record_epoch);
    uint32_t record_ordinal = header->xct_id_.get_ordinal();
    ASSERT_ND(previous_ordinal <= record_ordinal);
    previous_ordinal = record_ordinal;
  }
  ASSERT_ND(cur == bytes);
#endif  // NDEBUG
}

void Logger::assert_consistent() {
#ifndef NDEBUG
  ASSERT_ND(get_durable_epoch().is_valid());
  ASSERT_ND(!engine_->get_xct_manager()->is_initialized() ||
    get_durable_epoch() < engine_->get_xct_manager()->get_current_global_epoch());
  ASSERT_ND(control_block_->marked_epoch_.is_valid());
  ASSERT_ND(control_block_->marked_epoch_ <= get_durable_epoch().one_more());
  ASSERT_ND(is_log_aligned(control_block_->oldest_file_offset_begin_));
  ASSERT_ND(current_file_ == nullptr || is_log_aligned(current_file_->get_current_offset()));
  ASSERT_ND(is_log_aligned(control_block_->current_file_durable_offset_));
  ASSERT_ND(current_file_ == nullptr
    || control_block_->current_file_durable_offset_ <= current_file_->get_current_offset());
#endif  // NDEBUG
}

std::string Logger::to_string() const {
  std::stringstream stream;
  stream << *this;
  return stream.str();
}
std::ostream& operator<<(std::ostream& o, const Logger& v) {
  o << "<Logger>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<in_node_ordinal_>" << static_cast<int>(v.in_node_ordinal_) << "</in_node_ordinal_>"
    << "<log_folder_>" << v.log_folder_ << "</log_folder_>";
  o << "<assigned_thread_ids_>";
  for (auto thread_id : v.assigned_thread_ids_) {
    o << "<thread_id>" << thread_id << "</thread_id>";
  }
  o << "</assigned_thread_ids_>";
  o << "<durable_epoch_>" << v.get_durable_epoch() << "</durable_epoch_>"
    << "<marked_epoch_>" << v.control_block_->marked_epoch_ << "</marked_epoch_>"
    << "<oldest_ordinal_>" << v.control_block_->oldest_ordinal_ << "</oldest_ordinal_>"
    << "<oldest_file_offset_begin_>" << v.control_block_->oldest_file_offset_begin_
      << "</oldest_file_offset_begin_>"
    << "<current_ordinal_>" << v.control_block_->current_ordinal_ << "</current_ordinal_>";

  o << "<current_file_>";
  if (v.current_file_) {
    o << *v.current_file_;
  } else {
    o << "nullptr";
  }
  o << "</current_file_>";

  o << "<current_file_path_>" << v.current_file_path_ << "</current_file_path_>";

  o << "<current_file_length_>";
  if (v.current_file_) {
    o << v.current_file_->get_current_offset();
  } else {
    o << "nullptr";
  }
  o << "</current_file_length_>";

  o << "<epoch_history_head>"
    << v.control_block_->epoch_history_head_ << "</epoch_history_head>";
  o << "<epoch_history_count>"
    << v.control_block_->epoch_history_count_ << "</epoch_history_count>";
  o << "</Logger>";
  return o;
}

}  // namespace log
}  // namespace foedus
