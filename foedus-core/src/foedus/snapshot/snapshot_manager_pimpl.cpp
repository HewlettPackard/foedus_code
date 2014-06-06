/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/snapshot_manager_pimpl.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
#include <glog/logging.h>
namespace foedus {
namespace snapshot {
ErrorStack SnapshotManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing SnapshotManager..";
     if (!engine_->get_log_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
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

}  // namespace snapshot
}  // namespace foedus
