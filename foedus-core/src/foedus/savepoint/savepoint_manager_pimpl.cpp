/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/savepoint/savepoint_manager_pimpl.hpp>
#include <foedus/savepoint/savepoint_options.hpp>
#include <glog/logging.h>
namespace foedus {
namespace savepoint {
ErrorStack SavepointManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing SavepointManager..";
    return RET_OK;
}

ErrorStack SavepointManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing SavepointManager..";
    ErrorStackBatch batch;
    return RET_OK;
}

}  // namespace savepoint
}  // namespace foedus
