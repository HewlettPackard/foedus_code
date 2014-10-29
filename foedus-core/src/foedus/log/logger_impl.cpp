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
#include "foedus/log/thread_log_buffer_impl.hpp"
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
  control_block_->marked_epoch_ = get_durable_epoch().one_more();
  control_block_->no_log_epoch_ = false;
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
      soc::SharedMutexScope scope(control_block_->wakeup_cond_.get_mutex());
      control_block_->stop_requested_ = true;
      control_block_->wakeup_cond_.signal(&scope);
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
      soc::SharedMutexScope scope(control_block_->wakeup_cond_.get_mutex());
      if (!is_stop_requested()) {
        control_block_->wakeup_cond_.timedwait(&scope, 10000000ULL);
      }
    }
    const int kMaxIterations = 100;
    int iterations = 0;
    while (!is_stop_requested()) {
      assert_consistent();
      bool more_log_to_process = false;
      COERCE_ERROR(handle_logger_once(&more_log_to_process));
      if (!more_log_to_process) {
        break;
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

ErrorStack Logger::handle_logger_once(bool *more_log_to_process) {
  *more_log_to_process = false;
  assorted::spinlock_yield();
  CHECK_ERROR(update_durable_epoch());
  Epoch current_logger_epoch = get_durable_epoch().one_more();
  for (thread::Thread* the_thread : assigned_threads_) {
    if (control_block_->stop_requested_) {
      break;
    }

    // we FIRST take offset_committed with memory fence for the reason below.
    ThreadLogBuffer& buffer = the_thread->get_thread_log_buffer();
    assorted::memory_fence_acquire();
    const uint64_t offset_committed = buffer.get_offset_committed();
    if (offset_committed == buffer.get_offset_durable()) {
      VLOG(1) << "Thread-" << the_thread->get_thread_id() << " has no log to flush.";
      DVLOG(2) << *this;
      continue;
    }
    VLOG(0) << "Thread-" << the_thread->get_thread_id() << " has "
      << (offset_committed - buffer.get_offset_durable()) << " bytes logs to flush.";
    DVLOG(1) << *this;
    assorted::memory_fence_acquire();

    // (if we need to) we consume epoch mark AFTER the fence. Thus, we don't miss a
    // case where the thread adds a new epoch mark after we read offset_committed.
    buffer.consume_epoch_mark_as_many();

    if (buffer.offset_committed_ == buffer.offset_durable_) {
      // Then, the buffer is empty and maybe the worker thread is idle
      continue;
    }

    // we skip log buffer that went faster than others.
    // this guarantees that all log entries in this log file are ordered by epoch,
    // which simplifies log gleaners.
    if (buffer.logger_epoch_ > current_logger_epoch) {
      VLOG(0) << "Thread-" << the_thread->get_thread_id() << " already went to ep-"
        << buffer.logger_epoch_ << ". skip it. current=" << current_logger_epoch;
      *more_log_to_process = true;
      continue;
    }

    // okay, let's write out logs in this buffer
    uint64_t upto_offset;
    if (!buffer.logger_epoch_open_ended_) {
      // We know where current epoch logs end. Write up to there.
      upto_offset = buffer.logger_epoch_ends_;
    } else {
      // We don't know where it ends, but know that all logs are in current epoch.
      // then, we write out upto offset_committed_. however, consider the case:
      // 1) buffer has no mark (open ended) durable=10, committed=20, ep=3.
      // 2) this logger comes by with current_epoch=3. Sees no mark in buffer.
      // 3) buffer receives new log in the meantime, ep=4, new mark added,
      //   and committed is now 30. (10-20=ep3, 20-30=ep4 logs)
      // 4) logger "okay, I will flush out all logs up to committed(30)".
      // 5) logger writes out all logs up to 30, as **ep=3**.
      // To prevent this case, we first read offset_committed, take fence, then
      // check epoch mark.
      upto_offset = offset_committed;  // use the already-copied value
    }

    assorted::memory_fence_acquire();
    if (upto_offset != buffer.offset_durable_) {
      CHECK_ERROR(write_log(&buffer, upto_offset));
    }
    assorted::memory_fence_acquire();
    if (buffer.offset_durable_ != buffer.offset_committed_) {
      *more_log_to_process = true;
    }
  }

  CHECK_ERROR(update_durable_epoch());

  if (control_block_->marked_epoch_update_requested_) {
    control_block_->marked_epoch_update_requested_ = false;
    LOG(INFO) << "Logger-" << id_ << ": someone requested me to write out an epoch mark right now.";
    if (control_block_->marked_epoch_ == get_durable_epoch().one_more()) {
      LOG(INFO) << "Logger-" << id_ << " but the marked epoch is already the latest. nothing to do";
    } else {
      LOG(INFO) << "Logger-" << id_ << " Okay, okay, will do";
      control_block_->no_log_epoch_ = false;  // to forcibly write out the epoch mark this case
      CHECK_ERROR(log_epoch_switch(get_durable_epoch().one_more()));
    }
  }
  return kRetOk;
}

Epoch Logger::calculate_min_durable_epoch() {
  assert_consistent();
  const Epoch current_global_epoch = engine_->get_xct_manager()->get_current_global_epoch();
  ASSERT_ND(current_global_epoch.is_valid());
  assorted::memory_fence_acquire();  // necessary. following is AFTER this.

  VLOG(1) << "Logger-" << id_ << " update_durable_epoch(). durable_epoch_=" << get_durable_epoch()
    << ", current_global=" << current_global_epoch;
  DVLOG(2) << *this;

  Epoch min_durable_epoch = current_global_epoch.one_less();
  for (thread::Thread* the_thread : assigned_threads_) {
    ThreadLogBuffer& buffer = the_thread->get_thread_log_buffer();
    DVLOG(1) << "Check durable epoch(cur min=" << min_durable_epoch << "): "
      << "Thread-" << the_thread->get_thread_id() << ": " << buffer;
    buffer.consume_epoch_mark_as_many();
    if (buffer.logger_epoch_ > min_durable_epoch) {
      VLOG(1) << "Thread-" << the_thread->get_thread_id() << " at least durable up to "
        << min_durable_epoch;
      continue;
    }

    // in the worst case, buffer is durable up to only logger_epoch_ - 1. let's check.
    if (!buffer.logger_epoch_open_ended_) {
      // we know up to where we have to flush log to make it durable.
      ASSERT_ND(buffer.logger_epoch_ < current_global_epoch);  // thus it's not the latest
      if (buffer.logger_epoch_ends_ != buffer.offset_durable_) {
        // there are logs we have to flush, so surely logger_epoch_ - 1.
        VLOG(1) << "Thread-" << the_thread->get_thread_id() << " more logs in this ep";
        min_durable_epoch.store_min(buffer.logger_epoch_.one_less());
      } else {
        // Then no chance the thread adds more log to this epoch, good!
        VLOG(1) << "Thread-" << the_thread->get_thread_id() << " no more logs in this ep";
        min_durable_epoch.store_min(buffer.logger_epoch_);
        buffer.consume_epoch_mark();  // we can probably update buffer.logger_epoch_, too.
      }
    } else {
      VLOG(1) << "Thread-" << the_thread->get_thread_id() << " open-ended in this ep";
      // we are not sure whether we flushed everything in this epoch.
      // check if the thread _might_ be in commit phase.
      xct::Xct& xct = the_thread->get_current_xct();
      Epoch in_commit_epoch = xct.get_in_commit_log_epoch();
      // See get_in_commit_log_epoch() about the protocol of in_commit_log_epoch
      if (!in_commit_epoch.is_valid() || in_commit_epoch > buffer.logger_epoch_) {
        VLOG(1) << "Thread-" << the_thread->get_thread_id() << " not in a racy state!";
        assorted::memory_fence_acquire();  // because offset_committed might be added
        if (buffer.offset_durable_ == buffer.offset_committed_) {
          VLOG(1) << "Okay, definitely no log to process. just idle";
          // then, we are duralble at least to current_global_epoch.one_less()
        } else {
          VLOG(1) << "mm, still some log in this epoch";
          min_durable_epoch.store_min(buffer.logger_epoch_.one_less());
        }
      } else {
        ASSERT_ND(in_commit_epoch == buffer.logger_epoch_);
        // This is rare. worth logging in details. In this case, we spin on it.
        // This state is guaranteed to quickly end.
        // By doing this, handle_logger() can use this method for waiting.
        LOG(INFO) << "Thread-" << the_thread->get_thread_id() << " is now publishing"
          " logs that might be in epoch-" << buffer.logger_epoch_;
        DLOG(INFO) << "this=" << *this << ", buffer=" << buffer;
        // @spinlock
        SPINLOCK_WHILE(in_commit_epoch.is_valid()
          && in_commit_epoch <= buffer.logger_epoch_) {
          in_commit_epoch = xct.get_in_commit_log_epoch();
        }
        LOG(INFO) << "Thread-" << the_thread->get_thread_id() << " is done with the log"
          ". " << buffer.logger_epoch_;
        DLOG(INFO) << "this=" << *this << ", buffer=" << buffer;

        // Just logging. we conservatively use one_less because the transaction
        // probably produced logs in the epoch. We have to flush them first.
        min_durable_epoch.store_min(buffer.logger_epoch_.one_less());
      }
    }
  }
  return min_durable_epoch;
}

ErrorStack Logger::update_durable_epoch() {
  Epoch min_durable_epoch = calculate_min_durable_epoch();
  DVLOG(1) << "Checked all loggers. min_durable_epoch=" << min_durable_epoch;
  if (min_durable_epoch > get_durable_epoch()) {
    VLOG(0) << "Logger-" << id_ << " updating durable_epoch_ from " << get_durable_epoch()
      << " to " << min_durable_epoch;
    // BEFORE updating the epoch, fsync the file AND the parent folder
    if (!fs::fsync(current_file_path_, true)) {
      return ERROR_STACK_MSG(kErrorCodeFsSyncFailed, to_string().c_str());
    }
    control_block_->current_file_durable_offset_ = current_file_->get_current_offset();
    VLOG(0) << "Logger-" << id_ << " fsynced the current file ("
      << control_block_->current_file_durable_offset_ << "  bytes so far) and its folder";
    DVLOG(0) << "Before: " << *this;
    assorted::memory_fence_release();  // announce it only AFTER above
    // This must be the only place to set durable_epoch_
    control_block_->durable_epoch_ = min_durable_epoch.value();

    // finally, let the log manager re-calculate the global durable epoch.
    // this may or may not result in new global durable epoch
    assorted::memory_fence_release();
    CHECK_ERROR(engine_->get_log_manager()->refresh_global_durable_epoch());
    DVLOG(0) << "After: " << *this;
  } else {
    VLOG(1) << "Logger-" << id_ << " couldn't update durable_epoch_";
    DVLOG(1) << *this;
  }

  assert_consistent();
  return kRetOk;
}
ErrorStack Logger::write_dummy_epoch_mark() {
  control_block_->no_log_epoch_ = false;  // to forcibly write out the epoch mark this case
  CHECK_ERROR(log_epoch_switch(get_durable_epoch().one_more()));
  LOG(INFO) << "Logger-" << id_ << " wrote out a dummy epoch marker at the beginning";
  return kRetOk;
}

ErrorStack Logger::log_epoch_switch(Epoch new_epoch) {
  ASSERT_ND(control_block_->marked_epoch_ <= new_epoch);
  VLOG(0) << "Writing epoch marker for Logger-" << id_
    << ". marked_epoch_=" << control_block_->marked_epoch_ << " new_epoch=" << new_epoch;
  DVLOG(1) << *this;

  if (control_block_->no_log_epoch_) {
    VLOG(0) << "Logger-" << id_ << " had no log in this epoch. not writing an epoch mark."
      << " durable ep=" << get_durable_epoch() << ", new_epoch=" << new_epoch
      << " marked ep=" << control_block_->marked_epoch_;
  } else {
    // Use fill buffer to write out the epoch mark log
    std::lock_guard<std::mutex> guard(epoch_switch_mutex_);
    char* buf = reinterpret_cast<char*>(fill_buffer_.get_block());
    EpochMarkerLogType* epoch_marker = reinterpret_cast<EpochMarkerLogType*>(buf);
    epoch_marker->populate(
      control_block_->marked_epoch_, new_epoch, numa_node_, in_node_ordinal_, id_,
                 control_block_->current_ordinal_, current_file_->get_current_offset());

    // Fill it up to 4kb and write. A bit wasteful, but happens only once per epoch
    FillerLogType* filler_log = reinterpret_cast<FillerLogType*>(buf
      + sizeof(EpochMarkerLogType));
    filler_log->populate(fill_buffer_.get_size() - sizeof(EpochMarkerLogType));

    WRAP_ERROR_CODE(current_file_->write(fill_buffer_.get_size(), fill_buffer_));
    control_block_->no_log_epoch_ = true;
    control_block_->marked_epoch_ = new_epoch;
    add_epoch_history(*epoch_marker);
  }
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

ErrorStack Logger::write_log(ThreadLogBuffer* buffer, uint64_t upto_offset) {
  assert_consistent();
  Epoch write_epoch = get_durable_epoch().one_more();
  if (control_block_->marked_epoch_ != write_epoch) {
    control_block_->no_log_epoch_ = false;
    CHECK_ERROR(log_epoch_switch(write_epoch));
    ASSERT_ND(control_block_->marked_epoch_ == write_epoch);
  }

  CHECK_ERROR(switch_file_if_required());
  control_block_->no_log_epoch_ = false;
  uint64_t from_offset = buffer->get_offset_durable();
  VLOG(0) << "Writing out Thread-" << buffer->get_thread_id() << "'s log. from_offset="
    << from_offset << ", upto_offset=" << upto_offset << ", write_epoch=" << write_epoch;
  DVLOG(1) << *this;
  if (from_offset == upto_offset) {
    return kRetOk;
  }

  if (from_offset > upto_offset) {
    // this means wrap-around in the buffer
    // let's write up to the end of the circular buffer, then from the beginning.
    VLOG(0) << "Wraps around. from_offset=" << from_offset << ", upto_offset=" << upto_offset;
    DVLOG(0) << *this;

    // we can simply write out logs upto the end without worrying about the case where a log
    // entry spans the end of circular buffer. Because we avoid that in ThreadLogBuffer.
    // (see reserve_new_log()). So, we can separately handle the two writes by calling itself
    // again, which adds padding if they need.
    CHECK_ERROR(write_log(buffer, buffer->buffer_size_));
    ASSERT_ND(buffer->get_offset_durable() == 0);
    CHECK_ERROR(write_log(buffer, upto_offset));
    ASSERT_ND(buffer->get_offset_durable() == upto_offset);
    return kRetOk;
  }

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
    std::memcpy(buf, buffer->buffer_ + buffer->offset_durable_, copy_size);
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

    buffer->advance_offset_durable(copy_size);
  }

  from_offset = buffer->get_offset_durable();
  if (from_offset == upto_offset || (from_offset == 0 && upto_offset == buffer->buffer_size_)) {
    return kRetOk;
  }
  // from here, "from" is assured to be aligned
  ASSERT_ND(is_log_aligned(from_offset));
  ASSERT_ND(from_offset <= upto_offset);

  // 2) Middle regions where everything is aligned. easy
  uint64_t middle_size = align_log_floor(upto_offset) - from_offset;
  if (middle_size > 0) {
    memory::AlignedMemorySlice subslice(buffer->buffer_memory_, from_offset, middle_size);
    {
      debugging::StopWatch watch;
      VLOG(1) << "Writing middle regions: " << middle_size << " bytes. slice=" << subslice;
      WRAP_ERROR_CODE(current_file_->write(middle_size, subslice));
      watch.stop();
      // Maybe VLOG(0). but we need this information for the paper
      LOG(INFO) << "Wrote middle regions of " << middle_size << " bytes in "
        << watch.elapsed_ms() << "ms";
    }
    buffer->advance_offset_durable(middle_size);
  }

  from_offset = buffer->get_offset_durable();
  if (from_offset == upto_offset || (from_offset == 0 && upto_offset == buffer->buffer_size_)) {
    return kRetOk;  // if upto_offset is luckily aligned, we exit here.
  }
  ASSERT_ND(is_log_aligned(from_offset));
  ASSERT_ND(from_offset <= upto_offset);
  ASSERT_ND(!is_log_aligned(upto_offset));

  // 3) the last 4kb
  VLOG(1) << "padding at end needed.";
  char* buf = reinterpret_cast<char*>(fill_buffer_.get_block());

  uint64_t copy_size = upto_offset - from_offset;
  ASSERT_ND(copy_size < FillerLogType::kLogWriteUnitSize);
  std::memcpy(buf, buffer->buffer_ + buffer->offset_durable_, copy_size);
  buf += copy_size;

  // pad upto from_offset
  const uint64_t fill_size = FillerLogType::kLogWriteUnitSize - copy_size;
  FillerLogType* filler_log = reinterpret_cast<FillerLogType*>(buf);
  filler_log->populate(fill_size);

  CHECK_ERROR(current_file_->write(FillerLogType::kLogWriteUnitSize, fill_buffer_));
  buffer->advance_offset_durable(copy_size);
  return kRetOk;
}

void Logger::assert_consistent() {
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
    << "<no_log_epoch_>" << v.control_block_->no_log_epoch_ << "</no_log_epoch_>"
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
