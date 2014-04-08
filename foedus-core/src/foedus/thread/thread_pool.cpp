/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_options.hpp>
namespace foedus {
namespace thread {
ThreadPool::ThreadPool(const ThreadOptions &options) : options_(options) {
}
ErrorStack ThreadPool::initialize_once() {
    return RET_OK;
}

ErrorStack ThreadPool::uninitialize_once() {
    return RET_OK;
}

}  // namespace thread
}  // namespace foedus
