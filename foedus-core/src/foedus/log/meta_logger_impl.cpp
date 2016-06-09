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
#include "foedus/log/meta_logger_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type_invoke.hpp"
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
  CHECK_ERROR(truncate_non_durable(engine_->get_savepoint_manager()->get_saved_durable_epoch()));
  ASSERT_ND(control_block_->durable_offset_ == current_file_->get_current_offset());

  stop_requested_ = false;
  logger_thread_ = std::move(std::thread(&MetaLogger::meta_logger_main, this));
  return kRetOk;
}


void on_non_durable_meta_log_found(
  const log::LogHeader* entry,
  Epoch durable_epoch,
  uint64_t offset) {
  std::stringstream ss;
  ss << "    <Log offset=\"" << assorted::Hex(offset) << "\""
    << " len=\"" << assorted::Hex(entry->log_length_) << "\""
    << " type=\"" << assorted::Hex(entry->log_type_code_) << "\""
    << " storage_id=\"" << assorted::Hex(entry->storage_id_) << "\"";
  if (entry->log_length_ >= 8U) {
    ss << " xct_id_epoch=\"" << entry->xct_id_.get_epoch_int() << "\"";
    ss << " xct_id_ordinal=\"" << entry->xct_id_.get_ordinal() << "\"";
  }
  ss << ">";
  log::invoke_ostream(entry, &ss);
  ss << "</Log>";
  LOG(WARNING) << "Found a meta log that is not in durable epoch (" << durable_epoch
    << "). Probably the last run didn't invoke wait_for_commit(). The operation is discarded."
    << " Log content:" << std::endl << ss.str();
}

ErrorStack MetaLogger::truncate_non_durable(Epoch saved_durable_epoch) {
  ASSERT_ND(saved_durable_epoch.is_valid());
  const uint64_t from_offset = control_block_->oldest_offset_;
  const uint64_t to_offset = control_block_->durable_offset_;
  ASSERT_ND(from_offset <= to_offset);
  LOG(INFO) << "Truncating non-durable meta logs, if any. Right now meta logger's"
    << " oldest_offset_=" << from_offset
    << ", (meta logger's local) durable_offset_=" << to_offset
    << ", global saved_durable_epoch=" << saved_durable_epoch;
  ASSERT_ND(current_file_->is_opened());

  // Currently, we need to read everything from oldest_offset_ to see from where
  // we have non-durable logs, whose epochs are larger than the _globally_ durable epoch.
  // TASK(Hideaki) We should change SavepointManager to emit globally_durable_offset_. later.
  const uint64_t read_size = to_offset - from_offset;
  if (read_size > 0) {
    memory::AlignedMemory buffer;
    buffer.alloc(read_size, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
    WRAP_ERROR_CODE(current_file_->seek(from_offset, fs::DirectIoFile::kDirectIoSeekSet));
    WRAP_ERROR_CODE(current_file_->read_raw(read_size, buffer.get_block()));

    char* buf = reinterpret_cast<char*>(buffer.get_block());
    uint64_t cur = 0;
    uint64_t first_non_durable_at = read_size;
    while (cur < read_size) {
      log::BaseLogType* entry = reinterpret_cast<log::BaseLogType*>(buf + cur);
      ASSERT_ND(entry->header_.get_kind() != log::kRecordLogs);
      const uint32_t log_length = entry->header_.log_length_;
      log::LogCode type = entry->header_.get_type();
      ASSERT_ND(type != log::kLogCodeInvalid);
      if (type == log::kLogCodeFiller || type == log::kLogCodeEpochMarker) {
        // Skip filler/marker. These don't have XID
      } else {
        Epoch epoch = entry->header_.xct_id_.get_epoch();
        if (epoch <= saved_durable_epoch) {
          // Mostly this case.
        } else {
          // Ok, found a non-durable entry!
          const uint64_t raw_offset = from_offset + cur;
          on_non_durable_meta_log_found(&entry->header_, saved_durable_epoch, raw_offset);
          ASSERT_ND(first_non_durable_at == read_size || first_non_durable_at < cur);
          first_non_durable_at = std::min(first_non_durable_at, cur);
          // We can break here, but let's read all and warn all of them. meta log should be tiny
        }
      }
      cur += log_length;
    }

    if (first_non_durable_at < read_size) {
      // NOTE: This happens. Although the meta logger itself immediately flushes all logs
      // to durable storages, the global durable_epoch is min(all_logger_durable_epoch).
      // Thus, when the user didn't invoke wait_on_commit, we might have to discard
      // some meta logs that are "durable by itself" but "non-durable regarding the whole database"
      LOG(WARNING) << "Found some meta logs that are not in durable epoch (" << saved_durable_epoch
        << "). We will truncate non-durable regions. new durable_offset=" << first_non_durable_at;
      control_block_->durable_offset_ = first_non_durable_at;
      engine_->get_savepoint_manager()->change_meta_logger_durable_offset(first_non_durable_at);
    }
  } else {
    if (control_block_->durable_offset_ < current_file_->get_current_offset()) {
      // Even if all locally-durable regions are globally durable,
      // there still could be locally-non-durable regions (=not yet fsynced).
      // Will truncate such regions.
      LOG(ERROR) << "Meta log file has a non-durable region. Probably there"
        << " was a crash. Will truncate";
    }
  }

  const uint64_t new_offset = control_block_->durable_offset_;
  if (new_offset < current_file_->get_current_offset()) {
    LOG(WARNING) << "Truncating meta log file to " << new_offset
      << " from " << current_file_->get_current_offset();
    WRAP_ERROR_CODE(current_file_->truncate(new_offset, true));
  }
  WRAP_ERROR_CODE(current_file_->seek(new_offset, fs::DirectIoFile::kDirectIoSeekSet));
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
