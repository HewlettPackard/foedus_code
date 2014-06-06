/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/restart/restart_manager_pimpl.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <glog/logging.h>
namespace foedus {
namespace restart {
ErrorStack RestartManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing RestartManager..";
     if (!engine_->get_xct_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }

    // after all other initializations, we trigger recovery procedure.
    CHECK_ERROR(recover());
    return RET_OK;
}

ErrorStack RestartManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing RestartManager..";
    ErrorStackBatch batch;
    if (!engine_->get_xct_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    return RET_OK;
}

ErrorStack RestartManagerPimpl::recover() {
    LOG(INFO) << "Recovery the database...";
    return RET_OK;
}


}  // namespace restart
}  // namespace foedus
