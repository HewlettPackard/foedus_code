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
#include "foedus/restart/restart_manager_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/snapshot/snapshot_manager_pimpl.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace restart {
ErrorStack RestartManagerPimpl::initialize_once() {
  if (!engine_->get_xct_manager()->is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }
  control_block_ = engine_->get_soc_manager()->get_shared_memory_repo()->
    get_global_memory_anchors()->restart_manager_memory_;

  // Restart manager works only in master
  if (engine_->is_master()) {
    LOG(INFO) << "Initializing RestartManager..";

    // after all other initializations, we trigger recovery procedure.
    CHECK_ERROR(recover());
  }
  return kRetOk;
}

ErrorStack RestartManagerPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  if (!engine_->get_xct_manager()->is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  if (engine_->is_master()) {
    LOG(INFO) << "Uninitializing RestartManager..";
    // restart manager essentially has nothing to release, but because this is the first module
    // to uninit, we place "stop them first" kind of operations here.
    engine_->get_snapshot_manager()->get_pimpl()->stop_snapshot_thread();  // stop snapshot thread
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack RestartManagerPimpl::recover() {
  Epoch durable_epoch = engine_->get_log_manager()->get_durable_global_epoch();
  Epoch snapshot_epoch = engine_->get_snapshot_manager()->get_snapshot_epoch();
  LOG(INFO) << "Recovering the database... durable_epoch=" << durable_epoch
    << ", snapshot_epoch=" << snapshot_epoch;

  if (durable_epoch.value() == Epoch::kEpochInitialDurable) {
    if (!snapshot_epoch.is_valid()) {
      LOG(INFO) << "The database is in initial state. Nothing to recover.";
      return kRetOk;
    } else {
      // this means durable_epoch wraps around. nothing wrong, but worth logging.
      LOG(INFO) << "Interesting. durable_epoch is initial value, but we have snapshot."
        << " This means epoch wrapped around!";
    }
  }

  if (durable_epoch == snapshot_epoch) {
    LOG(INFO) << "The snapshot is up-to-date. No need to recover.";
    return kRetOk;
  }

  LOG(INFO) << "There are logs that are durable but not yet snapshotted.";
  CHECK_ERROR(redo_meta_logs(durable_epoch, snapshot_epoch));
  LOG(INFO) << "Launching snapshot..";
  snapshot::SnapshotManagerPimpl* snapshot_pimpl = engine_->get_snapshot_manager()->get_pimpl();
  snapshot::Snapshot the_snapshot;
  CHECK_ERROR(snapshot_pimpl->handle_snapshot_triggered(&the_snapshot));
  LOG(INFO) << "Finished initial snapshot during start-up.";

  // This fixes Bug #127.
  // Right after the recovery-snapshot, any non-null volatile root pages becomes stale.
  // we need to replace them with the new snapshot pages.
  CHECK_ERROR(engine_->get_storage_manager()->reinitialize_for_recovered_snapshot());

  LOG(INFO) << "Now we can start processing transaction";
  return kRetOk;
}

ErrorStack RestartManagerPimpl::redo_meta_logs(Epoch durable_epoch, Epoch snapshot_epoch) {
  ASSERT_ND(!snapshot_epoch.is_valid() || snapshot_epoch < durable_epoch);
  LOG(INFO) << "Redoing metadata operations from " << snapshot_epoch << " to " << durable_epoch;

  // Because metadata log is tiny, we do nothing complex here. Just read them all.
  fs::Path path(engine_->get_options().log_.construct_meta_log_path());
  fs::DirectIoFile file(path, engine_->get_options().log_.emulation_);
  WRAP_ERROR_CODE(file.open(true, false, false, false));
  uint64_t oldest_offset;
  uint64_t durable_offset;
  engine_->get_savepoint_manager()->get_meta_logger_offsets(&oldest_offset, &durable_offset);
  ASSERT_ND(oldest_offset % (1 << 12) == 0);
  ASSERT_ND(durable_offset % (1 << 12) == 0);
  ASSERT_ND(oldest_offset <= durable_offset);
  ASSERT_ND(fs::file_size(path) >= durable_offset);
  uint32_t read_size = durable_offset - oldest_offset;

  LOG(INFO) << "Will read " << read_size << " bytes from meta log";

  // Assuming it's tiny, just read it in one shot.
  memory::AlignedMemory buffer;
  buffer.alloc(read_size, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);

  WRAP_ERROR_CODE(file.seek(oldest_offset, fs::DirectIoFile::kDirectIoSeekSet));
  WRAP_ERROR_CODE(file.read_raw(read_size, buffer.get_block()));

  char* buf = reinterpret_cast<char*>(buffer.get_block());
  uint32_t cur = 0;
  uint32_t processed = 0;
  storage::StorageManager* stm = engine_->get_storage_manager();
  while (cur < read_size) {
    log::BaseLogType* entry = reinterpret_cast<log::BaseLogType*>(buf + cur);
    ASSERT_ND(entry->header_.get_kind() != log::kRecordLogs);
    const uint32_t log_length = entry->header_.log_length_;
    cur += log_length;
    log::LogCode type = entry->header_.get_type();
    ASSERT_ND(type != log::kLogCodeInvalid);
    if (type == log::kLogCodeFiller || type == log::kLogCodeEpochMarker) {
      continue;
    }
    Epoch epoch = entry->header_.xct_id_.get_epoch();
    if (epoch > durable_epoch) {
      LOG(FATAL) << "WTF. This should have been checked in meta logger's startup??";
    }
    if (snapshot_epoch.is_valid() && epoch <= snapshot_epoch) {
      continue;
    }
    switch (type) {
      case log::kLogCodeDropLogType:
        LOG(INFO) << "Redoing DROP STORAGE-" << entry->header_.storage_id_;
        stm->drop_storage_apply(entry->header_.storage_id_);
        ++processed;
        break;
      case log::kLogCodeArrayCreate:
      case log::kLogCodeSequentialCreate:
      case log::kLogCodeHashCreate:
      case log::kLogCodeMasstreeCreate:
        reinterpret_cast<storage::CreateLogType*>(entry)->apply_storage(
          engine_,
          entry->header_.storage_id_);
        ++processed;
        break;
      default:
        LOG(ERROR) << "Unexpected log type in metadata log:" << entry->header_;
    }
  }

  file.close();
  LOG(INFO) << "Redone " << processed << " metadata operations";
  return kRetOk;
}

}  // namespace restart
}  // namespace foedus
