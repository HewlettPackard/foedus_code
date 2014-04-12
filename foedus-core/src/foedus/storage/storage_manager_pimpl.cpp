/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/storage/storage_manager_pimpl.hpp>
#include <foedus/storage/storage_options.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <glog/logging.h>
namespace foedus {
namespace storage {
ErrorStack StorageManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing StorageManager..";
    if (!engine_->get_thread_pool().is_initialized()
        || !engine_->get_log_manager().is_initialized()
    ) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    return RET_OK;
}

ErrorStack StorageManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing StorageManager..";
    ErrorStackBatch batch;
    if (!engine_->get_thread_pool().is_initialized()
        || !engine_->get_log_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    return RET_OK;
}

}  // namespace storage
}  // namespace foedus
