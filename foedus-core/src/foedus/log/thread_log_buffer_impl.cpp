/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/log/thread_log_buffer_impl.hpp"

#include <glog/logging.h>

#include <chrono>
#include <list>
#include <ostream>
#include <thread>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"

namespace foedus {
namespace log {
ThreadLogBuffer::ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id)
  : engine_(engine), thread_id_(thread_id) {
  buffer_ = nullptr;
  buffer_size_ = 0;
}
ErrorStack ThreadLogBuffer::initialize_once() {
  memory::NumaCoreMemory *memory = engine_->get_memory_manager().get_core_memory(thread_id_);
  buffer_memory_ = memory->get_log_buffer_memory();
  buffer_ = reinterpret_cast<char*>(buffer_memory_.get_block());
  buffer_size_ = buffer_memory_.get_size();
  buffer_size_safe_ = buffer_size_ - 64;

  const savepoint::Savepoint &savepoint = engine_->get_savepoint_manager().get_savepoint_fast();
  last_epoch_ = savepoint.get_current_epoch();
  logger_epoch_ = savepoint.get_current_epoch();
  logger_epoch_ends_ = 0;
  logger_epoch_open_ended_ = true;
  offset_head_ = 0;
  offset_durable_ = 0;
  offset_committed_ = 0;
  offset_tail_ = 0;
  return kRetOk;
}

ErrorStack ThreadLogBuffer::uninitialize_once() {
  buffer_memory_.clear();
  buffer_ = nullptr;
  thread_epoch_marks_.clear();
  return kRetOk;
}

void ThreadLogBuffer::assert_consistent() const {
  ASSERT_ND(last_epoch_.is_valid());
  ASSERT_ND(logger_epoch_.is_valid());
  ASSERT_ND(last_epoch_ >= logger_epoch_);
  ASSERT_ND(offset_head_ < buffer_size_
    && offset_durable_ < buffer_size_
    && offset_committed_ < buffer_size_
    && offset_tail_ < buffer_size_);
  // because of wrap around, *at most* one of them does not hold
  int violation_count = 0;
  if (offset_head_ > offset_durable_) {
    ++violation_count;
  }
  if (offset_durable_ > offset_committed_) {
    ++violation_count;
  }
  if (offset_committed_ > offset_tail_) {
    ++violation_count;
  }
  ASSERT_ND(violation_count <= 1);
}

void ThreadLogBuffer::wait_for_space(uint16_t required_space) {
  LOG(INFO) << "Thread-" << thread_id_ << " waiting for space to write logs..";
  // @spinlock, but with a sleep (not in critical path, usually).
  while (head_to_tail_distance() + required_space >= buffer_size_safe_) {
    assorted::memory_fence_acquire();
    if (offset_durable_ != offset_head_) {
      // TODO(Hideaki) actually we should kick axx of log gleaner in this case.
      LOG(INFO) << "Thread-" << thread_id_ << " moving head to durable: " << *this;
      assorted::memory_fence_release();
      offset_head_ = offset_durable_;
      assorted::memory_fence_release();
    } else {
      LOG(WARNING) << "Thread-" << thread_id_ << " logger is getting behind. sleeping "
        << " for a while.." << *this;
      engine_->get_log_manager().wakeup_loggers();
      // TODO(Hideaki) this duration should be configurable.
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  ASSERT_ND(head_to_tail_distance() + required_space < buffer_size_safe_);
}

void ThreadLogBuffer::fillup_tail() {
  uint64_t len = buffer_size_ - offset_tail_;
  if (head_to_tail_distance() + len >= buffer_size_safe_) {
    wait_for_space(len);
  }
  ASSERT_ND(head_to_tail_distance() + len < buffer_size_safe_);
  FillerLogType *filler = reinterpret_cast<FillerLogType*>(buffer_ + offset_tail_);
  filler->populate(len);
  advance(buffer_size_, &offset_tail_, len);
  ASSERT_ND(offset_tail_ == 0);
}

void ThreadLogBuffer::on_new_epoch_observed(Epoch commit_epoch) {
  ASSERT_ND(commit_epoch > last_epoch_);
  VLOG(0) << "Thread-" << thread_id_ << " wrote out the first log entry in epoch-" << commit_epoch
    << " at offset " << offset_committed_ << ". old epoch=" << last_epoch_;
  DVLOG(0) << "Before: " << *this;
  ThreadEpockMark mark;
  mark.new_epoch_ = commit_epoch;
  mark.old_epoch_ = last_epoch_;
  mark.offset_epoch_begin_ = offset_committed_;
  last_epoch_ = commit_epoch;

  if (logger_epoch_ > mark.old_epoch_) {
    LOG(INFO) << "Thread-" << thread_id_ << " realized that its last_epoch_ is older"
      " than logger_epoch_, most likely this thread has been idle for a while";
    // Check if that's the case..
    ASSERT_ND(offset_committed_ == offset_durable_);
  }

  {
    std::lock_guard<std::mutex> guard(thread_epoch_marks_mutex_);
    thread_epoch_marks_.emplace_back(mark);
  }
  DVLOG(0) << "After: " << *this;
}

void ThreadLogBuffer::crash_stale_commit_epoch(Epoch commit_epoch) {
  LOG(FATAL) << "Received a log-publication request with commit_epoch=" << commit_epoch
    << ", which is older than logger_epoch_=" << logger_epoch_ << ", this is a BUG!"
    << std::endl << " Buffer=" << *this;
}

bool ThreadLogBuffer::consume_epoch_mark() {
  // this is called only when the logger has nothing to flush for the logger_epoch_
  ASSERT_ND(logger_epoch_open_ended_ || logger_epoch_ends_ == offset_durable_);
  ThreadEpockMark mark;
  bool last_mark;
  uint64_t next_mark_begin = 0;  // set when last_mark is false
  {
    std::lock_guard<std::mutex> guard(thread_epoch_marks_mutex_);
    if (thread_epoch_marks_.empty()) {
      return false;
    }

    mark = thread_epoch_marks_.front();
    if (logger_epoch_open_ended_ && offset_durable_ != mark.offset_epoch_begin_) {
      // Then, it's too early to consume this epoch mark. We are still working on the
      // current epoch.
      VLOG(1) << "There is a new epoch mark, but this still has logs in the current epoch"
        << " to process. this=" << *this;
      return false;
    }
    thread_epoch_marks_.pop_front();
    last_mark = thread_epoch_marks_.empty();
    if (!last_mark) {
      next_mark_begin = thread_epoch_marks_.front().offset_epoch_begin_;
    }
  }

  if (offset_durable_ != mark.offset_epoch_begin_) {
    LOG(FATAL) << "WHAT? Thread-" << thread_id_ << "'s log buffer is inconsistent."
      << " offset_durable_=" << offset_durable_ << ". but next marker begins at "
        << mark.offset_epoch_begin_ << ". this=" << *this;
  }

  logger_epoch_ = mark.new_epoch_;
  logger_epoch_open_ended_ = last_mark;
  logger_epoch_ends_ = next_mark_begin;
  return true;
}
void ThreadLogBuffer::consume_epoch_mark_as_many() {
  while (logger_epoch_open_ended_ || logger_epoch_ends_ == offset_durable_) {
    bool consumed = consume_epoch_mark();
    if (!consumed) {
      break;
    }
  }
}

void ThreadLogBuffer::list_uncommitted_logs(std::vector< char* >* out) {
  ASSERT_ND(out);
  out->clear();
  uint64_t cur = offset_committed_;
  uint64_t end = offset_tail_;
  while (cur != end) {
    out->push_back(buffer_ + cur);
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer_ + cur);
    if (header->log_length_ == 0 || header->log_length_ % 8 != 0) {
      LOG(FATAL) << "Invalid log length. Bug? " << header->log_length_;
    }
    cur += header->log_length_;
    if (cur > buffer_size_) {
      cur -= buffer_size_;
    }

    if (offset_committed_ < offset_tail_) {
      // if tail does not wrap around
      ASSERT_ND(offset_committed_ < cur && cur <= offset_tail_);
    } else {
      // if tail wraps around
      ASSERT_ND((offset_committed_ < cur && offset_tail_ < cur)  // before wrap around
        || (cur < offset_committed_ && cur <= offset_tail_));  // after wrap around
    }
  }
}


std::ostream& operator<<(std::ostream& o, const ThreadLogBuffer& v) {
  o << "<ThreadLogBuffer>";
  o << "<thread_id_>" << v.thread_id_ << "</thread_id_>";
  o << "<buffer_size_>" << v.buffer_size_ << "</buffer_size_>";
  o << "<offset_head_>" << v.offset_head_ << "</offset_head_>";
  o << "<offset_durable_>" << v.offset_durable_ << "</offset_durable_>";
  o << "<offset_committed_>" << v.offset_committed_ << "</offset_committed_>";
  o << "<offset_tail_>" << v.offset_tail_ << "</offset_tail_>";
  o << "<last_epoch_>" << v.last_epoch_ << "</last_epoch_>";
  o << "<logger_epoch_>" << v.logger_epoch_ << "</logger_epoch_>";
  o << "<logger_epoch_open_ended_>" << v.logger_epoch_open_ended_
    << "</logger_epoch_open_ended_>";
  o << "<logger_epoch_ends_>" << v.logger_epoch_ends_ << "</logger_epoch_ends_>";
  o << "<thread_epoch_marks_>";
  int cnt = 0;
  for (auto mark : v.thread_epoch_marks_) {
    o << mark;
    ++cnt;
    if (cnt == 20) {
      o << "...";
      break;
    }
  }
  o << "</thread_epoch_marks_>";
  o << "</ThreadLogBuffer>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const ThreadLogBuffer::ThreadEpockMark& v) {
  o << "<ThreadEpockMark>";
  o << "<old_epoch_>" << v.old_epoch_ << "</old_epoch_>";
  o << "<new_epoch_>" << v.new_epoch_ << "</new_epoch_>";
  o << "<offset_epoch_begin_>" << v.offset_epoch_begin_ << "</offset_epoch_begin_>";
  o << "</ThreadEpockMark>";
  return o;
}

}  // namespace log
}  // namespace foedus
