/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
namespace foedus {
namespace thread {

ImpersonateSession::ImpersonateSession(ImpersonateTask* task) {
    pimpl_ = new ImpersonateSessionPimpl(task);
}
ImpersonateSession::ImpersonateSession(const ImpersonateSession& other) {
    pimpl_ = new ImpersonateSessionPimpl(*other.pimpl_);
}
ImpersonateSession::~ImpersonateSession() {
    delete pimpl_;
    pimpl_ = nullptr;
}
ImpersonateSession& ImpersonateSession::operator=(const ImpersonateSession& other) {
    *pimpl_ = *other.pimpl_;
    return *this;
}

ImpersonateTask* ImpersonateSession::get_task() const { return pimpl_->task_; }
bool ImpersonateSession::is_valid() const { return pimpl_->is_valid(); }
ErrorStack ImpersonateSession::get_invalid_cause() const { return pimpl_->failure_cause_; }
void ImpersonateSession::set_invalid_cause(ErrorStack cause) { pimpl_->failure_cause_ = cause; }
ErrorStack ImpersonateSession::get_result() { return pimpl_->result_future_.get(); }
void ImpersonateSession::wait() const { return pimpl_->result_future_.wait(); }
ImpersonateSession::Status ImpersonateSession::wait_for(TimeoutMicrosec timeout) const {
    return pimpl_->wait_for(timeout);
}


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

ImpersonateSession ThreadPool::impersonate(ImpersonateTask* functor, TimeoutMicrosec timeout) {
    return pimpl_->impersonate(functor, timeout);
}

ImpersonateSession ThreadPool::impersonate_on_numa_core(
    ImpersonateTask* functor, ThreadId numa_core, TimeoutMicrosec timeout) {
    return pimpl_->impersonate_on_numa_core(functor, numa_core, timeout);
}

ImpersonateSession ThreadPool::impersonate_on_numa_node(
    ImpersonateTask* functor, ThreadGroupId numa_node, TimeoutMicrosec timeout) {
    return pimpl_->impersonate_on_numa_node(functor, numa_node, timeout);
}

}  // namespace thread
}  // namespace foedus
