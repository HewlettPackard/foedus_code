/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/thread/thread_pimpl.hpp>
#include <thread>
namespace foedus {
namespace thread {
ErrorStack ThreadPimpl::initialize_once() {
    core_memory_ = engine_->get_memory().get_core_memory(id_);
    raw_thread_ = new std::thread();
    if (raw_thread_ == nullptr) {
        return ERROR_STACK(ERROR_CODE_OUTOFMEMORY);
    }

    return RET_OK;
}
ErrorStack ThreadPimpl::uninitialize_once() {
    delete raw_thread_;
    raw_thread_ = NULL;
    core_memory_ = NULL;
    return RET_OK;
}

}  // namespace thread
}  // namespace foedus
