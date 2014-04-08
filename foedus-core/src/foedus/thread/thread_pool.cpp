/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
namespace foedus {
namespace thread {
ThreadPool::ThreadPool(Engine *engine) {
    pimpl_ = new ThreadPoolPimpl(engine);
}
ThreadPool::~ThreadPool() {
    delete pimpl_;
    pimpl_ = NULL;
}

ErrorStack ThreadPool::initialize() { return pimpl_->initialize(); }
bool ThreadPool::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack ThreadPool::uninitialize() { return pimpl_->uninitialize(); }
}  // namespace thread
}  // namespace foedus
