/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread_pimpl.hpp>
namespace foedus {
namespace thread {
ThreadPimpl::ThreadPimpl(ThreadGroup* group, thread_id id)
    : group_(group), id_(id), raw_thread_(nullptr), initialized_(false) {
}
ThreadPimpl::~ThreadPimpl() {
}
}  // namespace thread
}  // namespace foedus
