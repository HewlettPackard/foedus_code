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
#include "foedus/thread/thread_ref.hpp"

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
  explicit ThreadPoolPimpl(Engine* engine) : engine_(engine), local_group_(nullptr) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ImpersonateSession  impersonate(ImpersonateTask* functor);
  ImpersonateSession  impersonate_on_numa_node(ImpersonateTask* functor, ThreadGroupId numa_node);
  ImpersonateSession  impersonate_on_numa_core(ImpersonateTask* functor, ThreadId numa_core);

  ThreadGroupRef*     get_group(ThreadGroupId numa_node) { return &groups_[numa_node]; }
  ThreadGroup*        get_local_group() const { return local_group_; }
  ThreadRef*          get_thread(ThreadId id);

  friend  std::ostream& operator<<(std::ostream& o, const ThreadPoolPimpl& v);

  Engine* const               engine_;

  /**
   * Thread group of the local SOC engine.
   * If this is a master engine, null.
   */
  ThreadGroup*                local_group_;

  /**
   * List of all thread groups, one for each NUMA node in this engine.
   * Index is ThreadGroupId.
   */
  std::vector<ThreadGroupRef> groups_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
