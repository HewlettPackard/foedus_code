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
#include <foedus/savepoint/savepoint_manager_pimpl.hpp>
#include <foedus/savepoint/savepoint_options.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <foedus/log/log_manager.hpp>
#include <glog/logging.h>
#include <mutex>
namespace foedus {
namespace savepoint {
ErrorStack SavepointManagerPimpl::initialize_once() {
    savepoint_ = Savepoint();
    savepoint_path_ = fs::Path(engine_->get_options().savepoint_.savepoint_path_);
    LOG(INFO) << "Initializing SavepointManager.. path=" << savepoint_path_;
    auto logger_count = engine_->get_options().log_.loggers_per_node_;
    if (fs::exists(savepoint_path_)) {
        LOG(INFO) << "Existing savepoint file found. Loading..";
        CHECK_ERROR(savepoint_.load_from_file(savepoint_path_));
        if (!savepoint_.consistent(logger_count)) {
            return ERROR_STACK(ERROR_CODE_SP_INCONSISTENT_SAVEPOINT);
        }
    } else {
        LOG(INFO) << "Savepoint file does not exist. No savepoint taken so far.";
        // Create an empty savepoint file now. This makes sure the directory entry for the file
        // exists.
        savepoint_.populate_empty(logger_count);
        CHECK_ERROR(savepoint_.save_to_file(savepoint_path_));
    }
    return RET_OK;
}

ErrorStack SavepointManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing SavepointManager..";
    ErrorStackBatch batch;
    return RET_OK;
}

ErrorStack SavepointManagerPimpl::take_savepoint(Epoch new_global_durable_epoch) {
    Savepoint new_savepoint = Savepoint();
    assorted::memory_fence_acquire();
    new_savepoint.current_epoch_ = engine_->get_xct_manager().get_current_global_epoch().value();
    new_savepoint.durable_epoch_ = new_global_durable_epoch.value();
    engine_->get_log_manager().copy_logger_states(&new_savepoint);
    new_savepoint.assert_epoch_values();

    LOG(INFO) << "Writing a savepoint...";
    VLOG(0) << "Savepoint content=" << new_savepoint;
    CHECK_ERROR(new_savepoint.save_to_file(savepoint_path_));
    VLOG(0) << "Wrote a savepoint.";
    {
        std::lock_guard<std::mutex> guard(savepoint_mutex_);
        savepoint_ = new_savepoint;
    }
    return RET_OK;
}


const Savepoint& SavepointManagerPimpl::get_savepoint_fast() const { return savepoint_; }
Savepoint SavepointManagerPimpl::get_savepoint_safe() const {
    std::lock_guard<std::mutex> guard(savepoint_mutex_);
    Savepoint copied = savepoint_;
    return copied;
}

}  // namespace savepoint
}  // namespace foedus
