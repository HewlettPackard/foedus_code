/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/thread_pool.hpp"

#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"

namespace foedus {
namespace thread {

ThreadPool::ThreadPool(Engine *engine) : pimpl_(nullptr) {
  pimpl_ = new ThreadPoolPimpl(engine);
}
ThreadPool::~ThreadPool() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack ThreadPool::initialize() { return pimpl_->initialize(); }
bool ThreadPool::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack ThreadPool::uninitialize() { return pimpl_->uninitialize(); }

ImpersonateSession ThreadPool::impersonate(ImpersonateTask* functor) {
  return pimpl_->impersonate(functor);
}

ImpersonateSession ThreadPool::impersonate_on_numa_core(
  ImpersonateTask* functor,
  ThreadId numa_core) {
  return pimpl_->impersonate_on_numa_core(functor, numa_core);
}

ImpersonateSession ThreadPool::impersonate_on_numa_node(
  ImpersonateTask* functor,
  ThreadGroupId numa_node) {
  return pimpl_->impersonate_on_numa_node(functor, numa_node);
}

std::ostream& operator<<(std::ostream& o, const ThreadPool& v) {
  o << *v.pimpl_;
  return o;
}


}  // namespace thread
}  // namespace foedus
