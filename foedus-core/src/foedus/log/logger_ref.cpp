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
#include "foedus/log/logger_ref.hpp"

#include <glog/logging.h>

#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/logger_impl.hpp"
#include "foedus/savepoint/savepoint.hpp"

namespace foedus {
namespace log {
LoggerRef::LoggerRef() : Attachable<LoggerControlBlock>() {}
LoggerRef::LoggerRef(
  Engine* engine,
  LoggerControlBlock* block,
  LoggerId id,
  uint16_t numa_node,
  uint16_t in_node_ordinal)
  : Attachable<LoggerControlBlock>(engine, block) {
  id_ = id;
  numa_node_ = numa_node;
  in_node_ordinal_ = in_node_ordinal;
}

Epoch LoggerRef::get_durable_epoch() const {
  return Epoch(control_block_->durable_epoch_);
}

void LoggerRef::wakeup_for_durable_epoch(Epoch desired_durable_epoch) {
  assorted::memory_fence_acquire();
  if (get_durable_epoch() < desired_durable_epoch) {
    wakeup();
  }
}

void LoggerRef::wakeup() {
  control_block_->wakeup_cond_.signal();
}

void LoggerRef::copy_logger_state(savepoint::Savepoint* new_savepoint) const {
  new_savepoint->oldest_log_files_.push_back(control_block_->oldest_ordinal_);
  new_savepoint->oldest_log_files_offset_begin_.push_back(
    control_block_->oldest_file_offset_begin_);
  new_savepoint->current_log_files_.push_back(control_block_->current_ordinal_);
  new_savepoint->current_log_files_offset_durable_.push_back(
    control_block_->current_file_durable_offset_);
}

void LoggerRef::add_epoch_history(const EpochMarkerLogType& epoch_marker) {
  soc::SharedMutexScope scope(&control_block_->epoch_history_mutex_);
  uint32_t tail_index = control_block_->get_tail_epoch_history();
  ASSERT_ND(control_block_->epoch_history_count_ == 0
    || control_block_->epoch_histories_[tail_index].new_epoch_ ==  epoch_marker.old_epoch_);
  // the first epoch marker is allowed only if it's a dummy marker.
  // this simplifies the detection of first epoch marker
  if (!control_block_->is_epoch_history_empty()
      && epoch_marker.old_epoch_ == epoch_marker.new_epoch_) {
    LOG(INFO) << "Ignored a dummy epoch marker while replaying epoch marker log on Logger-"
      << id_ << ". marker=" << epoch_marker;
  } else {
    if (control_block_->epoch_history_count_ >= LoggerControlBlock::kMaxEpochHistory) {
      // TASK(Hideaki) To avoid this, we should maintain sparse history, like one history per
      // tens of epochs. So far kMaxEpochHistory is enough big, so it won't happen.
      LOG(FATAL) << "Exceeded kMaxEpochHistory. Unexpected.";
    }
    ++control_block_->epoch_history_count_;
    tail_index = control_block_->get_tail_epoch_history();
    control_block_->epoch_histories_[tail_index] = EpochHistory(epoch_marker);
    ASSERT_ND(control_block_->epoch_histories_[tail_index].new_epoch_.is_valid());
    ASSERT_ND(control_block_->epoch_histories_[tail_index].old_epoch_.is_valid());
  }
}


LogRange LoggerRef::get_log_range(Epoch prev_epoch, Epoch until_epoch) {
  // TASK(Hideaki) binary search. we assume there are not many epoch histories, so not urgent.
  ASSERT_ND(until_epoch <= get_durable_epoch());
  LogRange result;

  // Epoch mark is not written to empty epoch. Thus, we might holes at the beginning, in the
  // middle, and at the end. Be careful.
  soc::SharedMutexScope scope(&control_block_->epoch_history_mutex_);
  const uint32_t head = control_block_->epoch_history_head_;
  const uint32_t count = control_block_->epoch_history_count_;
  uint32_t pos = 0;  // RELATIVE position from head
  result.begin_file_ordinal = 0;
  result.begin_offset = 0;
  if (prev_epoch.is_valid()) {
    // first, locate the prev_epoch
    for (; pos < count; ++pos) {
      uint32_t abs_pos = control_block_->wrap_epoch_history_index(head + pos);
      const EpochHistory& cur = control_block_->epoch_histories_[abs_pos];
      if (cur.new_epoch_ > prev_epoch) {
        result.begin_file_ordinal = cur.log_file_ordinal_;
        result.begin_offset = cur.log_file_offset_;
        break;
      }
    }
    if (pos == count) {
      LOG(FATAL) << "No epoch mark found for " << prev_epoch << " in logger-" << id_;
    }
  }

  // next, locate until_epoch. we might not find it if the logger was idle for a while.
  // in that case, the last mark tells the file/ordinal.
  for (; pos < count; ++pos) {
    uint32_t abs_pos = control_block_->wrap_epoch_history_index(head + pos);
    const EpochHistory& cur = control_block_->epoch_histories_[abs_pos];
    // first mark that is after the until_epoch tells how much we have to read.
    // note that we might have multiple marks of the same epoch because of beginning-of-file marker.
    // we can't stop at the first mark with new_epoch==until.
    if (cur.new_epoch_ > until_epoch) {
      result.end_file_ordinal = cur.log_file_ordinal_;
      result.end_offset = cur.log_file_offset_;
      break;
    }
  }

  if (pos == count) {
    if (count == 0) {
      ASSERT_ND(result.begin_file_ordinal == 0);
      ASSERT_ND(result.begin_offset == 0);
      result.end_file_ordinal = 0;
      result.end_offset = 0;
    } else {
      // in this case, we read everything.
      result.end_file_ordinal = control_block_->current_ordinal_;
      result.end_offset = control_block_->current_file_durable_offset_;
    }
  }

  ASSERT_ND(result.begin_file_ordinal <= result.end_file_ordinal);
  ASSERT_ND(result.begin_file_ordinal < result.end_file_ordinal
    || result.begin_offset <= result.end_offset);
  return result;
}

}  // namespace log
}  // namespace foedus
