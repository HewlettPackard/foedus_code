/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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

  bool impersonate(
    const proc::ProcName& proc_name,
    const void* task_input,
    uint64_t task_input_size,
    ImpersonateSession *session);
  bool impersonate_on_numa_node(
    ThreadGroupId node,
    const proc::ProcName& proc_name,
    const void* task_input,
    uint64_t task_input_size,
    ImpersonateSession *session);
  bool impersonate_on_numa_core(
    ThreadId core,
    const proc::ProcName& proc_name,
    const void* task_input,
    uint64_t task_input_size,
    ImpersonateSession *session);

  ThreadGroupRef*     get_group(ThreadGroupId numa_node) { return &groups_[numa_node]; }
  ThreadGroup*        get_local_group() const { return local_group_; }
  ThreadRef*          get_thread(ThreadId id);

  /**
   * For better performance, but for some reason this method causes an issue in MCS lock.
   * We don't use this anywhere so far. Need to figure out what's wrong.
   */
  ThreadRef           get_thread_ref(ThreadId id) ALWAYS_INLINE;

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

inline ThreadRef ThreadPoolPimpl::get_thread_ref(ThreadId id) {
  ThreadGroupId numa_node = decompose_numa_node(id);
  ThreadLocalOrdinal core_ordinal = decompose_numa_local_ordinal(id);
  return groups_[numa_node].get_thread_ref(core_ordinal);
}

}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
