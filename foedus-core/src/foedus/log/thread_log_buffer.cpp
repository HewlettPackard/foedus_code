/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include "foedus/log/thread_log_buffer.hpp"

#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <list>
#include <ostream>
#include <thread>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/thread/thread_pimpl.hpp"

namespace foedus {
namespace log {

ThreadLogBufferMeta::ThreadLogBufferMeta() {
  thread_id_ = 0;
  buffer_size_ = 0;
  buffer_size_safe_ = 0;

  offset_head_ = 0;
  offset_durable_ = 0;
  offset_committed_ = 0;
  offset_tail_ = 0;

  std::memset(thread_epoch_marks_, 0, sizeof(thread_epoch_marks_));
  oldest_mark_index_ = 0;
  current_mark_index_ = 0;
}

void ThreadLogBufferMeta::assert_consistent() const {
#ifndef NDEBUG
  // These assertions might have a false positive unless run in a 1-threaded situation.
  // But, it would be negligibly rare, and anyway it is wiped out in release mode.
  // So, if you believe you are hitting assertion here because of a sheer luck, ignore.
  // In 99% cases these assersions fire for a valid reason, though.
  // Q: Making it thread-safe? A: That means even non-debug codepath becomes superslow.
  // Note, we thus use this method only from the worker thread itself. Then it's safe.
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
  ASSERT_ND(oldest_mark_index_ < static_cast<uint32_t>(kMaxNonDurableEpochs));
  ASSERT_ND(current_mark_index_ < static_cast<uint32_t>(kMaxNonDurableEpochs));

  for (uint32_t i = oldest_mark_index_; i != current_mark_index_; i = increment_mark_index(i)) {
    ThreadEpockMark mark = thread_epoch_marks_[i];
    ThreadEpockMark next = thread_epoch_marks_[increment_mark_index(i)];
    ASSERT_ND(mark.old_epoch_.is_valid());
    ASSERT_ND(mark.new_epoch_.is_valid());
    ASSERT_ND(mark.old_epoch_ < mark.new_epoch_);
    ASSERT_ND(mark.new_epoch_ == next.old_epoch_);
    ASSERT_ND(mark.offset_end_ == next.offset_begin_);
  }
  ThreadEpockMark cur = thread_epoch_marks_[current_mark_index_];
  ASSERT_ND(cur.old_epoch_.is_valid());
  ASSERT_ND(cur.new_epoch_.is_valid());
  ASSERT_ND(cur.old_epoch_ < cur.new_epoch_);
  ASSERT_ND(cur.offset_end_ == 0 || cur.offset_end_ == offset_committed_);
#endif  // NDEBUG
}


ThreadLogBuffer::ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id)
  : engine_(engine), meta_() {
  meta_.thread_id_ = thread_id;
  buffer_ = nullptr;
}

ErrorStack ThreadLogBuffer::initialize_once() {
  memory::NumaCoreMemory *memory
    = engine_->get_memory_manager()->get_local_memory()->get_core_memory(meta_.thread_id_);
  memory::AlignedMemorySlice buffer_memory = memory->get_log_buffer_memory();
  buffer_ = reinterpret_cast<char*>(buffer_memory.get_block());
  meta_.buffer_size_ = buffer_memory.get_size();
  meta_.buffer_size_safe_ = meta_.buffer_size_ - 64;

  meta_.offset_head_ = 0;
  meta_.offset_durable_ = 0;
  meta_.offset_committed_ = 0;
  meta_.offset_tail_ = 0;

  Epoch initial_current = engine_->get_savepoint_manager()->get_initial_current_epoch();
  Epoch initial_durable = engine_->get_savepoint_manager()->get_initial_durable_epoch();
  meta_.current_mark_index_ = 0;
  meta_.oldest_mark_index_ = 0;
  meta_.thread_epoch_marks_[0] = ThreadEpockMark(initial_durable, initial_current, 0);
  return kRetOk;
}

ErrorStack ThreadLogBuffer::uninitialize_once() {
  buffer_ = nullptr;
  return kRetOk;
}

void ThreadLogBuffer::wait_for_space(uint16_t required_space) {
  assert_consistent();
  LOG(INFO) << "Thread-" << meta_.thread_id_ << " waiting for space to write logs..";
  if (engine_->get_options().log_.emulation_.null_device_) {
    // logging disabled
    meta_.offset_head_ = meta_.offset_durable_ = meta_.offset_committed_;
    assorted::memory_fence_release();
    return;
  }
  // @spinlock, but with a sleep (not in critical path, usually).
  while (head_to_tail_distance() + required_space >= meta_.buffer_size_safe_) {
    assorted::memory_fence_acquire();
    if (meta_.offset_durable_ != meta_.offset_head_) {
      // TASK(Hideaki) actually we should kick axx of log gleaner in this case.
      LOG(INFO) << "Thread-" << meta_.thread_id_ << " moving head to durable: " << *this;
      assorted::memory_fence_release();
      meta_.offset_head_ = meta_.offset_durable_;
      assorted::memory_fence_release();
    } else {
      LOG(WARNING) << "Thread-" << meta_.thread_id_ << " logger is getting behind. sleeping "
        << " for a while.." << *this;
      engine_->get_log_manager()->wakeup_loggers();
      // TASK(Hideaki) this duration should be configurable.
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  ASSERT_ND(head_to_tail_distance() + required_space < meta_.buffer_size_safe_);
  assert_consistent();
}

void ThreadLogBuffer::fillup_tail() {
  assert_consistent();
  uint64_t len = meta_.buffer_size_ - meta_.offset_tail_;
  if (head_to_tail_distance() + len >= meta_.buffer_size_safe_) {
    wait_for_space(len);
  }
  ASSERT_ND(head_to_tail_distance() + len < meta_.buffer_size_safe_);
  FillerLogType *filler = reinterpret_cast<FillerLogType*>(buffer_ + meta_.offset_tail_);
  filler->populate(len);
  advance(meta_.buffer_size_, &meta_.offset_tail_, len);
  ASSERT_ND(meta_.offset_tail_ == 0);
  assert_consistent();
}

void ThreadLogBuffer::on_new_epoch_observed(Epoch commit_epoch) {
  assert_consistent();
  ThreadEpockMark& last_mark = meta_.thread_epoch_marks_[meta_.current_mark_index_];
  Epoch last_epoch = last_mark.new_epoch_;
  ASSERT_ND(commit_epoch > last_epoch);
  VLOG(0) << "Thread-" << meta_.thread_id_ << " is writing out the first log entry in epoch-"
    << commit_epoch
    << " at offset " << meta_.offset_committed_ << ". old epoch=" << last_epoch;
  DVLOG(0) << "Before: " << *this;
  ThreadEpockMark new_mark(last_epoch, commit_epoch, meta_.offset_committed_);

  // we will close the current mark, populating its offset_end_. It might be already
  // set by the logger while this thread was idle. In that case, the value must match.
  if (last_mark.offset_end_ != 0) {
    LOG(INFO) << "Interesting. Thread-" << meta_.thread_id_ << "'s last epoch marker was"
       << " already populated by the logger. This thread was probably idle for a while.";
    ASSERT_ND(last_mark.offset_end_ == meta_.offset_committed_);
  }
  last_mark.offset_end_ = meta_.offset_committed_;

  uint32_t new_index = ThreadLogBufferMeta::increment_mark_index(meta_.current_mark_index_);
  if (meta_.oldest_mark_index_ == new_index) {
    LOG(INFO) << "Thread-" << meta_.thread_id_ << " has to wait until the logger catches up."
      << " If this often happens, you should increase the number of loggers.";
    while (true) {
      // this doesn't happen often. So simply sleep for a while.
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      assorted::memory_fence_acquire();
      if (meta_.oldest_mark_index_ != new_index) {
        break;
      }
      VLOG(0) << "Thread-" << meta_.thread_id_ << " still waiting until the logger catches up...";
    }
  }
  ASSERT_ND(meta_.oldest_mark_index_ != new_index);

  assorted::memory_fence_release();
  meta_.thread_epoch_marks_[new_index] = new_mark;
  assorted::memory_fence_release();
  meta_.current_mark_index_ = new_index;
  assorted::memory_fence_release();

  DVLOG(0) << "After: " << *this;
  assert_consistent();
}

void ThreadLogBuffer::crash_stale_commit_epoch(Epoch commit_epoch) {
  assert_consistent();
  LOG(FATAL) << "Received a log-publication request with commit_epoch=" << commit_epoch
    << ", which is older than the last epoch=" << get_last_epoch() << ", this is a BUG!"
    << std::endl << " Buffer=" << *this;
  assert_consistent();
}

ThreadLogBuffer::OffsetRange ThreadLogBuffer::get_logs_to_write(Epoch written_epoch) {
  // See ThreadLogBufferMeta's class comment about tricky cases (the thread being idle).
  // assert_consistent(); this verification assumes the worker is not working. we can't use it here
  OffsetRange ret;
  ThreadEpockMark& target = meta_.thread_epoch_marks_[meta_.oldest_mark_index_];
  if (target.new_epoch_ > written_epoch) {
    // Case 1) no logs in this epoch.
    ASSERT_ND(target.offset_begin_ == meta_.offset_durable_);
    ret.begin_ = 0;
    ret.end_ = 0;
  } else if (target.new_epoch_ == written_epoch) {
    // Case 2) Is it 2-a? or 2-b?
    ASSERT_ND(target.offset_begin_ == meta_.offset_durable_);
    ret.begin_ = target.offset_begin_;
    if (meta_.oldest_mark_index_ != meta_.current_mark_index_) {
      // 2-a, easy.
      ret.end_ = target.offset_end_;
    } else {
      // 2-b, now we have to populate target.offset_end_ ourselves.
      VLOG(0) << "This guy seems sleeping for a while.." << *this;
      // We can safely populate target.offset_end_, but be careful on reading offset_committed_.
      // If the thread now resumes working, it adds a new mark *and then* increments
      // offset_committed_. So, it's safe as far as we take appropriate fences.

      // well, a bit redundant fences, but this part is not executed so often. no issue.
      assorted::memory_fence_acquire();
      uint64_t committed_copy = meta_.offset_committed_;
      assorted::memory_fence_acquire();
      bool still_current = meta_.oldest_mark_index_ == meta_.current_mark_index_;
      assorted::memory_fence_acq_rel();
      if (still_current) {
        target.offset_end_ = committed_copy;
        VLOG(0) << "Okay, the logger populated the offset_end on behalf." << *this;
      } else {
        // This is super-rare.
        LOG(INFO) << "Interesting. The thread has now awaken and added a new mark. offset_end"
          << " should be now populated. "<< *this;
      }

      assorted::memory_fence_acquire();
      ret.end_ = target.offset_end_;
    }
  } else {
    // Case 3) First, consume stale marks as much as possible
    // (note, in this case "target.offset_begin_ == meta_.offset_durable_" might not hold
    ASSERT_ND(target.new_epoch_ < written_epoch);
    if (meta_.oldest_mark_index_ != meta_.current_mark_index_) {
      VLOG(0) << "Advancing oldest mark index. before=" << *this;
      while (meta_.oldest_mark_index_ != meta_.current_mark_index_
          && meta_.thread_epoch_marks_[meta_.oldest_mark_index_].new_epoch_ < written_epoch) {
        meta_.oldest_mark_index_
          = ThreadLogBufferMeta::increment_mark_index(meta_.oldest_mark_index_);
        ASSERT_ND(meta_.oldest_mark_index_ < ThreadLogBufferMeta::kMaxNonDurableEpochs);
        if (meta_.oldest_mark_index_ >= ThreadLogBufferMeta::kMaxNonDurableEpochs) {
          LOG(FATAL) << "meta_.oldest_mark_index_ out of range. we must have waited.";
        }
      }
      VLOG(0) << "Advanced oldest mark index. after=" << *this;
      // Then re-evaluate. It might be now Case 1 or 2. recursion, but essentially a retry.
      ret = get_logs_to_write(written_epoch);
    } else {
      VLOG(1) << "Heh, this guy is still sleeping.." << *this;
      ret.begin_ = 0;
      ret.end_ = 0;
    }
  }
  return ret;
}

void ThreadLogBuffer::on_log_written(Epoch written_epoch) {
  // See ThreadLogBufferMeta's class comment about tricky cases (the thread being idle).
  // assert_consistent(); this verification assumes the worker is not working. we can't use it here

  ThreadEpockMark& target = meta_.thread_epoch_marks_[meta_.oldest_mark_index_];
  if (target.new_epoch_ > written_epoch) {
    // Case 1) no thing to do.
    ASSERT_ND(target.offset_begin_ == meta_.offset_durable_);
  } else if (target.new_epoch_ == written_epoch) {
    // Case 2) Is it 2-a? or 2-b?
    ASSERT_ND(target.offset_begin_ == meta_.offset_durable_);
    meta_.offset_durable_ = target.offset_end_;
    if (meta_.oldest_mark_index_ != meta_.current_mark_index_) {
      // 2-a, go to next mark.
      meta_.oldest_mark_index_
        = ThreadLogBufferMeta::increment_mark_index(meta_.oldest_mark_index_);
    } else {
      // 2-b, remains here. Will be case-3 next time.
    }
  } else {
    // Case 3 (even after consuming stale marks). Do nothing.
    ASSERT_ND(target.new_epoch_ < written_epoch);
  }
  DVLOG(0) << "Logger has written out all logs in epoch-" << written_epoch << ". " << *this;
  // assert_consistent(); this verification assumes the worker is not working. we can't use it here
}

std::ostream& operator<<(std::ostream& o, const ThreadLogBuffer& v) {
  o << v.meta_;
  return o;
}

std::ostream& operator<<(std::ostream& o, const ThreadLogBufferMeta& v) {
  o << "<ThreadLogBuffer>";
  o << "<thread_id_>" << v.thread_id_ << "</thread_id_>";
  o << "<buffer_size_>" << v.buffer_size_ << "</buffer_size_>";
  o << "<offset_head_>" << v.offset_head_ << "</offset_head_>";
  o << "<offset_durable_>" << v.offset_durable_ << "</offset_durable_>";
  o << "<offset_committed_>" << v.offset_committed_ << "</offset_committed_>";
  o << "<offset_tail_>" << v.offset_tail_ << "</offset_tail_>";
  o << "<thread_epoch_marks"
    << " oldest=\"" << v.oldest_mark_index_ << "\""
    << " current=\"" << v.current_mark_index_ << "\""
    << ">";

  // from oldest (inclusive) to current (exclusive).
  for (uint32_t i = v.oldest_mark_index_;
        i != v.current_mark_index_;
        i = ThreadLogBufferMeta::increment_mark_index(i)) {
    o << "<Entry index=\"" << i << "\">" << v.thread_epoch_marks_[i] << "</Entry>";
  }
  // also write out current entry
  o << "<Entry index=\"" << v.current_mark_index_ << "\">"
    << v.thread_epoch_marks_[v.current_mark_index_]
    << "</Entry>";
  o << "</thread_epoch_marks>";
  o << "</ThreadLogBuffer>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const ThreadEpockMark& v) {
  o << "<ThreadEpockMark "
    << " old=\"" << v.old_epoch_ << "\""
    << " new=\"" << v.new_epoch_ << "\""
    << " offset_begin=\"" << assorted::Hex(v.offset_begin_) << "\""
    << " offset_end=\"" << assorted::Hex(v.offset_end_) << "\""
    << " />";
  return o;
}

}  // namespace log
}  // namespace foedus
