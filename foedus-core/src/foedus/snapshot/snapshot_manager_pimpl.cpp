/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/snapshot/snapshot_manager_pimpl.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
#include <foedus/snapshot/snapshot_metadata.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/debugging/stop_watch.hpp>
#include <glog/logging.h>
#include <chrono>
#include <string>
namespace foedus {
namespace snapshot {
const SnapshotOptions& SnapshotManagerPimpl::get_option() const {
    return engine_->get_options().snapshot_;
}

ErrorStack SnapshotManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing SnapshotManager..";
     if (!engine_->get_log_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    snapshot_epoch_.store(Epoch::EPOCH_INVALID);
    // TODO(Hideaki): get snapshot status from savepoint
    previous_snapshot_id_ = NULL_SNAPSHOT_ID;
    immediate_snapshot_requested_.store(false);
    previous_snapshot_time_ = std::chrono::system_clock::now();
    snapshot_thread_.initialize("Snapshot",
                    std::thread(&SnapshotManagerPimpl::handle_snapshot, this),
                    std::chrono::milliseconds(100));
    return RET_OK;
}

ErrorStack SnapshotManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing SnapshotManager..";
    ErrorStackBatch batch;
    if (!engine_->get_log_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    snapshot_thread_.stop();
    return RET_OK;
}

void SnapshotManagerPimpl::handle_snapshot() {
    LOG(INFO) << "Snapshot thread started";
    // The actual snapshotting can't start until all other modules are initialized.
    SPINLOCK_WHILE(!snapshot_thread_.is_stop_requested() && !engine_->is_initialized()) {
        assorted::memory_fence_acquire();
    }

    LOG(INFO) << "Snapshot thread now starts taking snapshot";
    while (!snapshot_thread_.sleep()) {
        // should we start snapshotting? or keep sleeping?
        bool triggered = false;
        std::chrono::system_clock::time_point until = previous_snapshot_time_ +
            std::chrono::milliseconds(get_option().snapshot_interval_milliseconds_);
        Epoch durable_epoch = engine_->get_log_manager().get_durable_global_epoch();
        Epoch previous_epoch = get_snapshot_epoch();
        if (previous_epoch.is_valid() && previous_epoch == durable_epoch) {
            LOG(INFO) << "Current snapshot is already latest. durable_epoch=" << durable_epoch;
        } else if (immediate_snapshot_requested_) {
            // if someone requested immediate snapshot, do it.
            triggered = true;
            immediate_snapshot_requested_.store(false);
            LOG(INFO) << "Immediate snapshot request detected. snapshotting..";
        } else if (std::chrono::system_clock::now() >= until) {
            triggered = true;
            LOG(INFO) << "Snapshot interval has elapsed. snapshotting..";
        } else {
            // TODO(Hideaki): check free pages in page pool and compare with configuration.
        }

        if (triggered) {
            Snapshot new_snapshot;
            // TODO(Hideaki): graceful error handling
            COERCE_ERROR(handle_snapshot_triggered(&new_snapshot));
            Epoch new_snapshot_epoch = new_snapshot.valid_until_epoch_;
            ASSERT_ND(new_snapshot_epoch.is_valid() &&
                (!get_snapshot_epoch().is_valid() || new_snapshot_epoch > get_snapshot_epoch()));

            // done. notify waiters if exist
            Epoch::EpochInteger epoch_after = new_snapshot_epoch.value();
            previous_snapshot_id_ = new_snapshot.id_;
            previous_snapshot_time_ = std::chrono::system_clock::now();
            snapshot_taken_.notify_all([this, epoch_after]{ snapshot_epoch_.store(epoch_after); });
        } else {
            VLOG(1) << "Snapshotting not triggered. going to sleep again";
        }
    }

    LOG(INFO) << "Snapshot thread ended. ";
}

void SnapshotManagerPimpl::trigger_snapshot_immediate(bool wait_completion) {
    LOG(INFO) << "Requesting to immediately take a snapshot...";
    Epoch before = get_snapshot_epoch();
    Epoch durable_epoch = engine_->get_log_manager().get_durable_global_epoch();
    if (before.is_valid() && before == durable_epoch) {
        LOG(INFO) << "Current snapshot is already latest. durable_epoch=" << durable_epoch;
        return;
    }

    immediate_snapshot_requested_.store(true);
    snapshot_thread_.wakeup();
    if (wait_completion) {
        LOG(INFO) << "Waiting for the completion of snapshot... before=" << before;
        snapshot_taken_.wait([this, before]{ return before != get_snapshot_epoch(); });
        LOG(INFO) << "Observed the completion of snapshot! after=" << get_snapshot_epoch();
    }
}

ErrorStack SnapshotManagerPimpl::handle_snapshot_triggered(Snapshot *new_snapshot) {
    Epoch durable_epoch = engine_->get_log_manager().get_durable_global_epoch();
    Epoch previous_epoch = get_snapshot_epoch();
    LOG(INFO) << "Taking a new snapshot. durable_epoch=" << durable_epoch
        << ". previous_snapshot=" << previous_epoch;
    ASSERT_ND(durable_epoch.is_valid() && durable_epoch > previous_epoch);
    new_snapshot->base_epoch_ = previous_epoch;
    new_snapshot->valid_until_epoch_ = durable_epoch;

    // determine the snapshot ID
    SnapshotId snapshot_id;
    if (previous_snapshot_id_ == NULL_SNAPSHOT_ID) {
        snapshot_id = 1;
    } else {
        snapshot_id = increment(previous_snapshot_id_);
    }
    LOG(INFO) << "Issued ID for this snapshot:" << snapshot_id;
    new_snapshot->id_ = snapshot_id;

    CHECK_ERROR(snapshot_metadata(new_snapshot));

    return RET_OK;
}

ErrorStack SnapshotManagerPimpl::snapshot_metadata(Snapshot *new_snapshot) {
    // construct metadata object
    SnapshotMetadata metadata;
    metadata.id_ = new_snapshot->id_;
    metadata.base_epoch_ = new_snapshot->base_epoch_.value();
    metadata.valid_until_epoch_ = new_snapshot->valid_until_epoch_.value();
    CHECK_ERROR(engine_->get_storage_manager().duplicate_all_storage_metadata(&metadata));

    // save it to a file
    fs::Path folder(get_option().get_primary_folder_path());
    if (!fs::exists(folder)) {
        if (!fs::create_directories(folder, true)) {
            LOG(ERROR) << "Failed to create directory:" << folder << ". check permission.";
            return ERROR_STACK(ERROR_CODE_FS_MKDIR_FAILED);
        }
    }

    fs::Path file = get_snapshot_metadata_file_path(new_snapshot->id_);
    LOG(INFO) << "New snapshot metadata file fullpath=" << file;

    debugging::StopWatch stop_watch;
    CHECK_ERROR(metadata.save_to_file(file));
    stop_watch.stop();
    LOG(INFO) << "Wrote a snapshot metadata file. size=" << fs::file_size(file) << " bytes"
        << ", elapsed time to write=" << stop_watch.elapsed_ms() << "ms. now fsyncing...";
    stop_watch.start();
    fs::fsync(file, true);
    stop_watch.stop();
    LOG(INFO) << "fsynced the file and the folder! elapsed=" << stop_watch.elapsed_ms() << "ms.";
    return RET_OK;
}

fs::Path SnapshotManagerPimpl::get_snapshot_metadata_file_path(SnapshotId snapshot_id) const {
    fs::Path folder(get_option().get_primary_folder_path());
    fs::Path file(folder);
    file /= std::string("snapshot_metadata_")
        + std::to_string(snapshot_id) + std::string(".xml");
    return file;
}

}  // namespace snapshot
}  // namespace foedus
