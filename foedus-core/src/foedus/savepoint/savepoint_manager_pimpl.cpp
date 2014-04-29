/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/savepoint/savepoint_manager_pimpl.hpp>
#include <foedus/savepoint/savepoint_options.hpp>
#include <foedus/engine_options.hpp>
#include <glog/logging.h>
#include <mutex>
namespace foedus {
namespace savepoint {
ErrorStack SavepointManagerPimpl::initialize_once() {
    savepoint_ = Savepoint();
    savepoint_path_ = fs::Path(engine_->get_options().savepoint_.savepoint_path_);
    LOG(INFO) << "Initializing SavepointManager.. path=" << savepoint_path_;
    auto logger_count = engine_->get_options().log_.get_logger_count();
    if (fs::exists(savepoint_path_)) {
        LOG(INFO) << "Existing savepoint file found. Loading..";
        CHECK_ERROR(savepoint_.load_from_file(savepoint_path_));
        if (!savepoint_.consistent(logger_count)) {
            return ERROR_STACK(ERROR_CODE_SP_INCONSISTENT_SAVEPOINT);
        }
        if (savepoint_.empty()) {
            LOG(INFO) << "The savepoint file was empty.";
        }
    } else {
        LOG(INFO) << "Savepoint file does not exist. No savepoint taken so far.";
        // Create an empty savepoint file now. This makes sure the directory entry for the file
        // exists.
        savepoint_.populate_empty(logger_count);
        CHECK_ERROR(write_savepoint());
    }
    return RET_OK;
}

ErrorStack SavepointManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing SavepointManager..";
    ErrorStackBatch batch;
    return RET_OK;
}

ErrorStack SavepointManagerPimpl::write_savepoint() {
    CHECK_ERROR(savepoint_.save_to_file(savepoint_path_));
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
