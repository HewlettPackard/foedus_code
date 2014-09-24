/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  soc::SharedMutexScope scope(control_block_->wakeup_cond_.get_mutex());
  control_block_->wakeup_cond_.signal(&scope);
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
    ++control_block_->epoch_history_count_;
    tail_index = control_block_->get_tail_epoch_history();
    control_block_->epoch_histories_[tail_index] = EpochHistory(epoch_marker);
    ASSERT_ND(control_block_->epoch_histories_[tail_index].new_epoch_.is_valid());
    ASSERT_ND(control_block_->epoch_histories_[tail_index].old_epoch_.is_valid());
  }
}


LogRange LoggerRef::get_log_range(Epoch prev_epoch, Epoch until_epoch) {
  // TODO(Hideaki) binary search. we assume there are not many epoch histories, so not urgent.
  ASSERT_ND(until_epoch <= get_durable_epoch());
  LogRange result;

  // to make sure we have an epoch mark, we update marked_epoch_
  while (control_block_->marked_epoch_ <= until_epoch) {
    LOG(INFO) << "Logger-" << id_ << " does not have an epoch mark for "
      << until_epoch << ". Waiting for the logger to catch up...";
    control_block_->marked_epoch_update_requested_ = true;
    wakeup();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assorted::memory_fence_acquire();
  }
  ASSERT_ND(control_block_->marked_epoch_ > until_epoch);

  soc::SharedMutexScope scope(&control_block_->epoch_history_mutex_);
  uint32_t head = control_block_->epoch_history_head_;
  uint32_t count = control_block_->epoch_history_count_;
  uint32_t pos = 0;  // RELATIVE position from head
  result.begin_file_ordinal = 0;
  result.begin_offset = 0;
  if (prev_epoch.is_valid()) {
    // first, locate the prev_epoch
    for (; pos < count; ++pos) {
      const EpochHistory& cur = control_block_->epoch_histories_[
        control_block_->wrap_epoch_history_index(head + pos)];
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
  // in that case, the current file/ordinal is used.
  for (; pos < count; ++pos) {
    const EpochHistory& cur = control_block_->epoch_histories_[
      control_block_->wrap_epoch_history_index(head + pos)];
    if (cur.new_epoch_ > until_epoch) {
      result.end_file_ordinal = cur.log_file_ordinal_;
      result.end_offset = cur.log_file_offset_;
      break;
    }
  }

  if (pos == count && !result.is_empty()) {
    LOG(FATAL) << "No epoch mark found for " << until_epoch << " in logger-" << id_;
  }

  ASSERT_ND(result.begin_file_ordinal <= result.end_file_ordinal);
  ASSERT_ND(result.begin_file_ordinal < result.end_file_ordinal
    || result.begin_offset <= result.end_offset);
  return result;
}

}  // namespace log
}  // namespace foedus
