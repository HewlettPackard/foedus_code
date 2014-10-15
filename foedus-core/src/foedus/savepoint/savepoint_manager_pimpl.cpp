/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/savepoint/savepoint_manager_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_manager_pimpl.hpp"
#include "foedus/log/meta_log_buffer.hpp"
#include "foedus/savepoint/savepoint_options.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace savepoint {
ErrorStack SavepointManagerPimpl::initialize_once() {
  control_block_ = engine_->get_soc_manager()->get_shared_memory_repo()->
    get_global_memory_anchors()->savepoint_manager_memory_;
  if (engine_->is_master()) {
    // Savepoint takes place only in master
    control_block_->initialize();
    savepoint_ = Savepoint();
    savepoint_path_ = fs::Path(engine_->get_options().savepoint_.savepoint_path_.str());
    LOG(INFO) << "Initializing SavepointManager.. path=" << savepoint_path_;
    auto logger_count = engine_->get_options().log_.loggers_per_node_
      * engine_->get_options().thread_.group_count_;
    if (fs::exists(savepoint_path_)) {
      LOG(INFO) << "Existing savepoint file found. Loading..";
      CHECK_ERROR(savepoint_.load_from_file(savepoint_path_));
      if (!savepoint_.consistent(logger_count)) {
        return ERROR_STACK(kErrorCodeSpInconsistentSavepoint);
      }
    } else {
      LOG(INFO) << "Savepoint file does not exist. No savepoint taken so far.";
      // Create an empty savepoint file now. This makes sure the directory entry for the file
      // exists.
      savepoint_.populate_empty(logger_count);
      CHECK_ERROR(savepoint_.save_to_file(savepoint_path_));
    }
    update_shared_savepoint(savepoint_);
    control_block_->initial_current_epoch_ = savepoint_.current_epoch_;
    control_block_->initial_durable_epoch_ = savepoint_.durable_epoch_;
    control_block_->saved_durable_epoch_ = savepoint_.durable_epoch_;
    control_block_->requested_durable_epoch_ = savepoint_.durable_epoch_;
    savepoint_thread_stop_requested_ = false;
    assorted::memory_fence_release();
    savepoint_thread_ = std::move(std::thread(&SavepointManagerPimpl::savepoint_main, this));
    control_block_->master_initialized_ = true;
  } else {
    // other engines wait for the master engine until it finishes the initialization of
    // relevant fields. Some of the following modules depend on these values.
    uint32_t sleep_cont = 0;
    while (control_block_->master_initialized_ == false) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (++sleep_cont > 1000ULL) {
        return ERROR_STACK_MSG(kErrorCodeTimeout, "Master engine couldn't load savepoint??");
      }
    }
    LOG(INFO) << "Okay, master-engine has finished loading initial savepoint.";
  }
  return kRetOk;
}

ErrorStack SavepointManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing SavepointManager..";
  ErrorStackBatch batch;
  if (engine_->is_master()) {
    if (savepoint_thread_.joinable()) {
      {
        soc::SharedMutexScope scope(control_block_->save_wakeup_.get_mutex());
        savepoint_thread_stop_requested_ = true;
        control_block_->save_wakeup_.signal(&scope);
      }
      savepoint_thread_.join();
    }
    control_block_->uninitialize();
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack SavepointManagerPimpl::take_savepoint(Epoch new_global_durable_epoch) {
  while (get_saved_durable_epoch() < new_global_durable_epoch) {
    if (get_requested_durable_epoch() < new_global_durable_epoch) {
      soc::SharedMutexScope scope(control_block_->save_wakeup_.get_mutex());
      if (get_requested_durable_epoch() < new_global_durable_epoch) {
        control_block_->requested_durable_epoch_ = new_global_durable_epoch.value();
        control_block_->save_wakeup_.signal(&scope);
      }
    }
    {
      soc::SharedMutexScope scope(control_block_->save_done_event_.get_mutex());
      if (get_saved_durable_epoch() >= new_global_durable_epoch) {
        break;
      }
      control_block_->save_done_event_.wait(&scope);
    }
  }
  return kRetOk;
}
ErrorStack SavepointManagerPimpl::take_savepoint_after_snapshot(
  snapshot::SnapshotId new_snapshot_id,
  Epoch new_snapshot_epoch) {
  while (get_latest_snapshot_id() != new_snapshot_id) {
    {
      soc::SharedMutexScope scope(control_block_->save_wakeup_.get_mutex());
      control_block_->new_snapshot_id_ = new_snapshot_id;
      control_block_->new_snapshot_epoch_ = new_snapshot_epoch.value();
      control_block_->save_wakeup_.signal(&scope);
    }
    {
      soc::SharedMutexScope scope(control_block_->save_done_event_.get_mutex());
      control_block_->save_done_event_.wait(&scope);
    }
  }
  ASSERT_ND(get_latest_snapshot_id() == new_snapshot_id);
  ASSERT_ND(get_latest_snapshot_epoch() == new_snapshot_epoch);
  return kRetOk;
}

void SavepointManagerPimpl::update_shared_savepoint(const Savepoint& src) {
  // write with mutex to not let readers see garbage.
  // there is only one writer anyway, btw.
  soc::SharedMutexScope scope(&control_block_->savepoint_mutex_);
  control_block_->savepoint_.update(
    engine_->get_options().thread_.group_count_,
    engine_->get_options().log_.loggers_per_node_,
    src);
}

LoggerSavepointInfo SavepointManagerPimpl::get_logger_savepoint(log::LoggerId logger_id) {
  // read with mutex to not see garbage
  soc::SharedMutexScope scope(&control_block_->savepoint_mutex_);
  ASSERT_ND(logger_id < control_block_->savepoint_.get_total_logger_count());
  return control_block_->savepoint_.logger_info_[logger_id];
}

void SavepointManagerPimpl::savepoint_main() {
  LOG(INFO) << "Savepoint thread has started.";
  while (!is_stop_requested()) {
    {
      soc::SharedMutexScope scope(control_block_->save_wakeup_.get_mutex());
      if (!is_stop_requested() &&
        control_block_->requested_durable_epoch_ == control_block_->saved_durable_epoch_ &&
        control_block_->new_snapshot_id_ == snapshot::kNullSnapshotId) {
        control_block_->save_wakeup_.timedwait(&scope, 100000000ULL);
      }
    }
    if (is_stop_requested()) {
      break;
    }
    if (control_block_->new_snapshot_id_ != snapshot::kNullSnapshotId ||
      control_block_->requested_durable_epoch_ != control_block_->saved_durable_epoch_) {
      Savepoint new_savepoint;
      new_savepoint.current_epoch_ = engine_->get_xct_manager()->get_current_global_epoch().value();
      Epoch new_durable_epoch = get_requested_durable_epoch();
      new_savepoint.durable_epoch_ = new_durable_epoch.value();
      engine_->get_log_manager()->copy_logger_states(&new_savepoint);

      if (control_block_->new_snapshot_id_ != snapshot::kNullSnapshotId) {
        new_savepoint.latest_snapshot_id_ = control_block_->new_snapshot_id_;
        new_savepoint.latest_snapshot_epoch_ = control_block_->new_snapshot_epoch_;
        control_block_->new_snapshot_id_ = snapshot::kNullSnapshotId;
        control_block_->new_snapshot_epoch_ = Epoch::kEpochInvalid;
      } else {
        new_savepoint.latest_snapshot_id_ = control_block_->savepoint_.latest_snapshot_id_;
        new_savepoint.latest_snapshot_epoch_ = control_block_->savepoint_.latest_snapshot_epoch_;
      }

      log::MetaLogControlBlock* metalog_block = engine_->get_soc_manager()->get_shared_memory_repo()
        ->get_global_memory_anchors()->meta_logger_memory_;
      // TODO(Hideaki) Here, we should update oldest_offset_ by checking where the snapshot_epoch
      // ends. So far we don't update this, but metalog is anyway tiny, so isn't a big issue.
      new_savepoint.meta_log_oldest_offset_ = metalog_block->oldest_offset_;
      new_savepoint.meta_log_durable_offset_ = metalog_block->durable_offset_;
      new_savepoint.assert_epoch_values();

      VLOG(0) << "Writing a savepoint...";
      VLOG(1) << "Savepoint content=" << new_savepoint;
      COERCE_ERROR(new_savepoint.save_to_file(savepoint_path_));
      update_shared_savepoint(new_savepoint);  // also write to shared memory
      VLOG(1) << "Wrote a savepoint.";
      engine_->get_log_manager()->announce_new_durable_global_epoch(new_durable_epoch);
      {
        soc::SharedMutexScope scope(control_block_->save_done_event_.get_mutex());
        control_block_->saved_durable_epoch_ = new_durable_epoch.value();
        control_block_->save_done_event_.broadcast(&scope);
      }
    }
  }
  LOG(INFO) << "Savepoint thread has terminated.";
}

}  // namespace savepoint
}  // namespace foedus
