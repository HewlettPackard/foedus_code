/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/log/logger_ref.hpp"

#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/logger_impl.hpp"
#include "foedus/savepoint/savepoint.hpp"

namespace foedus {
namespace log {
LoggerRef::LoggerRef() : Attachable<LoggerControlBlock>() {}
LoggerRef::LoggerRef(Engine* engine, LoggerControlBlock* block)
  : Attachable<LoggerControlBlock>(engine, block) {}

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


}  // namespace log
}  // namespace foedus
