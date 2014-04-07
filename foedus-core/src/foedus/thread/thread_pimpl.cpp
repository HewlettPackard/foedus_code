/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread_pimpl.hpp>
namespace foedus {
namespace thread {
ThreadPimpl::ThreadPimpl(ThreadGroup* group, ThreadId id)
    : group_(group), id_(id), raw_thread_(nullptr), initialized_(false) {
}
ThreadPimpl::~ThreadPimpl() {
}
ErrorStack ThreadPimpl::initialize_once() {
    return RET_OK;
}
ErrorStack ThreadPimpl::uninitialize_once() {
    return RET_OK;
}

}  // namespace thread
}  // namespace foedus
