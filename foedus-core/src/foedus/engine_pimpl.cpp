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
    debug_(options.debugging_),
    filesystem_(options.fs_),
    initialized_(false) {
}
EnginePimpl::~EnginePimpl() {
}

ErrorStack EnginePimpl::initialize_once() {
    // initialize debugging module at the beginning. we can use glog since now.
    CHECK_ERROR(debug_.initialize());
    // other init
    return RET_OK;
}
ErrorStack EnginePimpl::uninitialize_once() {
    ErrorStackBatch batch;
    // other uninit
    // release debugging module at the end. we can't use glog since now.
    batch.emprace_back(debug_.uninitialize());
    return SUMMARIZE_ERROR_BATCH(batch);
}
}  // namespace foedus
