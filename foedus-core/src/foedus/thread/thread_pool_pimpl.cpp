/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread_pool_pimpl.hpp>
namespace foedus {
namespace thread {
ThreadPoolPimpl::ThreadPoolPimpl(const ThreadOptions &options) : options_(options) {
}
ErrorStack ThreadPoolPimpl::initialize_once() {
    return RET_OK;
}

ErrorStack ThreadPoolPimpl::uninitialize_once() {
    return RET_OK;
}

}  // namespace thread
}  // namespace foedus
