/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/log/meta_log_buffer.hpp"

#include <glog/logging.h>

#include <cstring>
#include <ostream>
#include <thread>

#include "foedus/engine.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace log {

void MetaLogBuffer::commit(BaseLogType* metalog, Epoch* commit_epoch) {
  LOG(INFO) << "Writing a metadata log. " << metalog->header_ << "...";
  {
    soc::SharedMutexScope scope(&control_block_->mutex_);
    // access to metadata buffer is mutex-protected
    ASSERT_ND(control_block_->buffer_used_ == 0);

    // To avoid mixing with normal operations on the storage in this epoch, advance epoch.
    // This happens within the mutex, so this is assured to be the only metadata log in the epoch.
    engine_->get_xct_manager().advance_current_global_epoch();
    *commit_epoch = engine_->get_xct_manager().get_current_global_epoch();
    LOG(INFO) << "Issued an epoch for the metadata log: " << *commit_epoch << "...";
    metalog->header_.xct_id_.set_epoch(*commit_epoch);

    // copy to buffer_ first
    ASSERT_ND(metalog->header_.get_kind() == kStorageLogs
      || metalog->header_.get_kind() == kEngineLogs);
    std::memcpy(control_block_->buffer_, metalog, metalog->header_.log_length_);

    // then write buffer_used_ after fence for the logger to safely read it
    // Also, do it within mutex to avoid lost signal
    {
      // Wakeup the logger
      soc::SharedMutexScope scope(control_block_->logger_wakeup_.get_mutex());
      control_block_->buffer_used_ = metalog->header_.log_length_;
      control_block_->logger_wakeup_.signal(&scope);
    }

    // Simply sleep for a while. Metadata logging is not so often, so we can simply spin
    // with a short sleep.
    while (control_block_->buffer_used_ > 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      assorted::memory_fence_acquire();
    }
  }
  LOG(INFO) << "Wrote a metadata log. " << metalog->header_ << "...";
}

std::ostream& operator<<(std::ostream& o, const MetaLogBuffer& v) {
  o << "<MetaLogBuffer>"
    << "<buffer_used_>" << v.control_block_->buffer_used_ << "</buffer_used_>"
    << "<oldest_offset_>" << v.control_block_->oldest_offset_ << "</oldest_offset_>"
    << "<durable_offset_>" << v.control_block_->durable_offset_ << "</durable_offset_>"
    << "</MetaLogBuffer>";
  return o;
}

}  // namespace log
}  // namespace foedus
