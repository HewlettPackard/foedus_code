/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
#include <iosfwd>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace thread {

/**
 * @brief Pimpl object of ThreadPool.
 * @ingroup THREADPOOL
 * @details
 * A private pimpl object for ThreadPool.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ThreadPoolPimpl final : public DefaultInitializable {
 public:
  ThreadPoolPimpl() = delete;
  explicit ThreadPoolPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ImpersonateSession  impersonate(ImpersonateTask* functor);
  ImpersonateSession  impersonate_on_numa_node(ImpersonateTask* functor, ThreadGroupId numa_node);
  ImpersonateSession  impersonate_on_numa_core(ImpersonateTask* functor, ThreadId numa_core);

  ThreadGroup*        get_group(ThreadGroupId numa_node) const;
  Thread*             get_thread(ThreadId id) const;

  friend  std::ostream& operator<<(std::ostream& o, const ThreadPoolPimpl& v);

  Engine* const               engine_;

  /**
   * List of ThreadGroup, one for each NUMA node in this engine.
   * Index is ThreadGroupId.
   */
  std::vector<ThreadGroup*>   groups_;

  /**
   * @brief Whether this thread pool has stopped allowing further impersonation.
   * @details
   * As the first step to terminate the entire engine, uninitialize() sets this to true,
   * prohibiting further impersonations from client code.
   */
  bool                        no_more_impersonation_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
