/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/log/meta_logger_impl.hpp"

#include <glog/logging.h>

#include <cstring>
#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/soc/soc_manager.hpp"

namespace foedus {
namespace log {

ErrorStack MetaLogger::initialize_once() {
  ASSERT_ND(engine_->is_master());
  control_block_ = engine_->get_soc_manager()->get_shared_memory_repo()
        ->get_global_memory_anchors()->meta_logger_memory_;
  control_block_->initialize();
  std::memset(control_block_->buffer_, 0, sizeof(control_block_->buffer_));

  fs::Path path(engine_->get_options().log_.construct_meta_log_path());
  if (!fs::exists(path.parent_path())) {
    fs::create_directories(path.parent_path());
  }

  engine_->get_savepoint_manager()->get_meta_logger_offsets(
    &control_block_->oldest_offset_,
    &control_block_->durable_offset_);

  // Open log file
  current_file_ = new fs::DirectIoFile(path, engine_->get_options().log_.emulation_);
  WRAP_ERROR_CODE(current_file_->open(true, true, true, true));
  if (control_block_->durable_offset_ < current_file_->get_current_offset()) {
    LOG(ERROR) << "Meta log file has a non-durable region. Probably there"
      << " was a crash. Will truncate it to " << control_block_->durable_offset_
      << " from " << current_file_->get_current_offset();
    WRAP_ERROR_CODE(current_file_->truncate(control_block_->durable_offset_, true));
  }
  ASSERT_ND(control_block_->durable_offset_ == current_file_->get_current_offset());

  stop_requested_ = false;
  logger_thread_ = std::move(std::thread(&MetaLogger::meta_logger_main, this));
  return kRetOk;
}

ErrorStack MetaLogger::uninitialize_once() {
  ASSERT_ND(engine_->is_master());
  if (logger_thread_.joinable()) {
    {
      stop_requested_ = true;
      control_block_->logger_wakeup_.signal();
    }
    logger_thread_.join();
  }
  control_block_->uninitialize();
  if (current_file_) {
    current_file_->close();
    delete current_file_;
    current_file_ = nullptr;
  }
  return kRetOk;
}

void MetaLogger::meta_logger_main() {
  LOG(INFO) << "Meta-logger started";
  while (!stop_requested_) {
    {
      uint64_t demand = control_block_->logger_wakeup_.acquire_ticket();
      if (!stop_requested_ && !control_block_->has_waiting_log()) {
        VLOG(0) << "Meta-logger going to sleep";
        control_block_->logger_wakeup_.timedwait(demand, 100000ULL);
      }
    }
    VLOG(0) << "Meta-logger woke up";
    if (stop_requested_) {
      break;
    } else if (!control_block_->has_waiting_log()) {
      continue;
    }

    // we observed buffer_used_ > 0 BEFORE the fence. Now we can read buffer_ safely.
    assorted::memory_fence_acq_rel();
    ASSERT_ND(control_block_->buffer_used_ > 0);
    uint32_t write_size = sizeof(control_block_->buffer_);
    ASSERT_ND(control_block_->buffer_used_ <= write_size);
    LOG(INFO) << "Meta-logger got a log (" << control_block_->buffer_used_ << " b) to write out";
    FillerLogType* filler = reinterpret_cast<FillerLogType*>(
      control_block_->buffer_ + control_block_->buffer_used_);
    filler->populate(write_size - control_block_->buffer_used_);
    ErrorCode er = current_file_->write_raw(write_size, control_block_->buffer_);
    if (er != kErrorCodeOk) {
      LOG(FATAL) << "Meta-logger couldn't write a log. error=" << get_error_message(er)
        << ", os_error=" << assorted::os_error();
    }

    // also fsync on the file and parent. every, single, time.
    // we don't care performance on metadata logger. just make it simple and robust.
    bool synced = fs::fsync(current_file_->get_path(), true);
    if (!synced) {
      LOG(FATAL) << "Meta-logger couldn't fsync. os_error=" << assorted::os_error();
    }
    assorted::memory_fence_release();
    control_block_->buffer_used_ = 0;
    assorted::memory_fence_release();
    control_block_->durable_offset_ += write_size;
  }
  LOG(INFO) << "Meta-logger terminated";
}

std::ostream& operator<<(std::ostream& o, const MetaLogger& v) {
  o << "<MetaLogger>"
    << "<current_file_>";
  if (v.current_file_) {
    o << *v.current_file_;
  } else {
    o << "nullptr";
  }
  o << "</current_file_>"
    << "</MetaLogger>";
  return o;
}

}  // namespace log
}  // namespace foedus
