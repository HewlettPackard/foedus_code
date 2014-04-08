/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_pimpl.hpp>
#include <foedus/error_stack_batch.hpp>
namespace foedus {

EnginePimpl::EnginePimpl(const EngineOptions &options) :
    options_(options),
    memory_(options),
    filesystem_(options.fs_),
    log_manager_(options.log_),
    thread_pool_(options.thread_),
    debug_(options.debugging_) {
}

ErrorStack EnginePimpl::initialize_once() {
    // initialize debugging module at the beginning. we can use glog since now.
    CHECK_ERROR(debug_.initialize());

    // other init
    CHECK_ERROR(filesystem_.initialize());
    CHECK_ERROR(log_manager_.initialize());
    CHECK_ERROR(memory_.initialize());
    CHECK_ERROR(thread_pool_.initialize());
    return RET_OK;
}
ErrorStack EnginePimpl::uninitialize_once() {
    ErrorStackBatch batch;
    // other uninit (reverse order)
    batch.emprace_back(thread_pool_.uninitialize());
    batch.emprace_back(memory_.uninitialize());
    batch.emprace_back(log_manager_.uninitialize());
    batch.emprace_back(filesystem_.uninitialize());

    // release debugging module at the end. we can't use glog since now.
    batch.emprace_back(debug_.uninitialize());
    return SUMMARIZE_ERROR_BATCH(batch);
}
}  // namespace foedus
