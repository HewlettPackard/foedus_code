/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/xct/xct_manager_pimpl.hpp>
#include <foedus/xct/xct_options.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <glog/logging.h>
namespace foedus {
namespace xct {
ErrorStack XctManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing XctManager..";
    if (!engine_->get_storage_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    return RET_OK;
}

ErrorStack XctManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing XctManager..";
    ErrorStackBatch batch;
    if (!engine_->get_storage_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    return RET_OK;
}

ErrorStack XctManagerPimpl::begin_xct(thread::Thread* context) {
    if (context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_ALREADY_RUNNING);
    }
    context->activate_xct();
    return RET_OK;
}
ErrorStack XctManagerPimpl::commit_xct(thread::Thread* context) {
    if (!context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }
    // TODO(Hideaki) Implement
    context->deactivate_xct();
    return RET_OK;
}
ErrorStack XctManagerPimpl::abort_xct(thread::Thread* context) {
    if (!context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }
    // TODO(Hideaki) Implement
    context->deactivate_xct();
    return RET_OK;
}

}  // namespace xct
}  // namespace foedus
