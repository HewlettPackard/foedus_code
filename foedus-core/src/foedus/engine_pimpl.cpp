/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_pimpl.hpp>
#include <foedus/error_stack_batch.hpp>
namespace foedus {

EnginePimpl::EnginePimpl(Engine* engine, const EngineOptions &options) :
    options_(options),
    engine_(engine),
    debug_(engine),
    filesystem_(engine),
    memory_manager_(engine),
    thread_pool_(engine),
    log_manager_(engine),
    storage_manager_(engine) {
}

ErrorStack EnginePimpl::initialize_once() {
    // initialize debugging module at the beginning. we can use glog since now.
    CHECK_ERROR(debug_.initialize());
    CHECK_ERROR(filesystem_.initialize());
    CHECK_ERROR(memory_manager_.initialize());
    CHECK_ERROR(thread_pool_.initialize());
    CHECK_ERROR(log_manager_.initialize());
    CHECK_ERROR(storage_manager_.initialize());
    return RET_OK;
}
ErrorStack EnginePimpl::uninitialize_once() {
    ErrorStackBatch batch;
    // other uninit (reverse order)
    batch.emprace_back(storage_manager_.uninitialize());
    batch.emprace_back(log_manager_.uninitialize());
    batch.emprace_back(thread_pool_.uninitialize());
    batch.emprace_back(memory_manager_.uninitialize());
    batch.emprace_back(filesystem_.uninitialize());
    batch.emprace_back(debug_.uninitialize());
    // release debugging module at the end. we can't use glog since now.
    return SUMMARIZE_ERROR_BATCH(batch);
}
}  // namespace foedus
