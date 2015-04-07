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
#ifndef FOEDUS_THREAD_THREAD_POOL_HPP_
#define FOEDUS_THREAD_THREAD_POOL_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/impersonate_session.hpp"

namespace foedus {
namespace thread {

/**
 * @defgroup THREADPOOL Thread-Pool and Impersonation
 * @brief APIs to \b pool and \b impersonate worker threads in libfoedus-core
 * @ingroup THREAD
 * @details
 * @section THR Database Engine API and Thread
 * You might have already noticed that most of our APIs provided by the database engine
 * requires a Thread object as its execution context.
 * On the other hand, client programs aren't allowed to instantiate Thread instances by themselves.
 * So, you have to first obtain the Thread object from the engine.
 * This document describes how user programs do that, and why.
 *
 * @section POOL Thread Pool
 * Most client program needs concurrent accesses running on multiple threads.
 * In order to avoid overheads and complexity of launching and maintaining threads
 * by such client programs, our engine provides a pool of pre-allocated threads.
 * This also makes it easy for the engine to guarantee efficient mapping between
 * thread and NUMA-core. As described in \ref MEMHIERARCHY, our engine ties threads to
 * their private memories. If our engine lets the client program to manage its own threads,
 * we can't achieve the static and efficient mapping.
 *
 * @par Number of thread/group configurations
 * Our engine pre-allocates a fixed number of ThreadGroup and Thread at start-up.
 * You can configure the numbers in ThreadOptions, but in many cases the default
 * values (which is the number of hardware NUMA nodes/cores) work fine.
 *
 * @section IMPERSONATE Impersonation
 * Our API to execute transactions consists of the following three concepts:
 *  \li \b Procedure (\ref PROC) is an arbitrary user-defined code to run in our engine.
 *  \li \b Session (ImpersonateSession) is a one-to-one mapping between a procedure and a pooled
 * thread that continues until the completion of the procedure.
 *  \li \b Impersonation (ThreadPool#impersonate()) is an action to create a session for the
 * given procedure.
 *
 * In order to start a user transaction or a series of user transactions on some thread,
 * the user first defines the procedure as a function as defined in \ref PROC.
 * Then, the user calls ThreadPool#impersonate() to impersonate one of the pre-allocated
 * threads for the procedure.
 *
 * When the thread is impersonated, it starts runninng the procedure asynchronously.
 * The original client thread, which invoked the impersonation, immediately returns
 * with ImpersonateSession object for the acquired session.
 * The original thread can choose to either synchronously wait for the completion of the procedure
 * or do other stuffs, such as launching more sessions (be it the same procedure or not).
 *
 * This means that a use code doesn't have to do \b anything to run the procedures on an arbitrary
 * number of threads. Everything is encapsulated in the ThreadPool class and its related classes.
 *
 * @section EX Examples
 * Below is a trivial example to define a procedure and submit impersonation request.
 * @code{.cpp}
 * #include "foedus/engine.hpp"
 * #include "foedus/engine_options.hpp"
 * #include "foedus/proc/proc_manager.hpp"
 * #include "foedus/thread/thread_pool.hpp"
 * #include "foedus/thread/thread.hpp"
 *
 * foedus::ErrorStack my_proc(const foedus::proc::ProcArguments& args) {
 *   std::cout << "Ya!" << std::endl;
 *   return foedus::kRetOk;
 * }
 *
 * int main(int argc, char **argv) {
 *   foedus::EngineOptions options;
 *   foedus::Engine engine(options);
 *   engine.get_proc_manager()->pre_register("my_proc", my_proc);
 *   if (engine.initialize().is_error()) {
 *     return 1;
 *   }
 *
 *   foedus::UninitializeGuard guard(&engine);
 *   foedus::ErrorStack result = engine.get_thread_pool()->impersonate_synchronous("my_proc");
 *   std::cout << "result=" << result << std::endl;
 *   if (engine.uninitialize().is_error()) {
 *     return 1;
 *   }
 *
 *   return 0;
 * }
 * @endcode
 */

/**
 * @brief The pool of pre-allocated threads in the engine to execute transactions.
 * @ingroup THREADPOOL
 * @details
 * This is the main API class of thread package.
 * Its gut is impersonate() which allows client programs to create a new session
 * (ImpersonateSession) that runs user-defined procedures (\ref PROC).
 * The sessions are executed on pre-allocated threads in the engine.
 * We throttle sessions, meaning impersonate() blocks when there is no available
 * thread. To avoid waiting too long, impersonate() receives timeout parameter.
 */
class ThreadPool CXX11_FINAL : public virtual Initializable {
 public:
  ThreadPool() CXX11_FUNC_DELETE;
  explicit ThreadPool(Engine *engine);
  ~ThreadPool();
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /**
   * @brief Impersonate as one of pre-allocated threads in this engine, executing
   * the procedure on the impersonated thread (\b NOT the current thread).
   * @param[in] proc_name the name of the procedure to run on this thread.
   * @param[in] task_input input data of arbitrary format for the procedure.
   * @param[in] task_input_size byte size of the input data to copy into the thread's memory.
   * @param[out] session the session to run on this thread. On success, the session receives a
   * ticket so that the caller can wait for the completion.
   * @details
   * This is similar to launch a new thread that calls the functor.
   * The difference is that this doesn't actually create a thread (which is very expensive)
   * but instead just impersonates as one of the pre-allocated threads in the engine.
   * @return whether successfully impersonated.
   */
  bool impersonate(
    const proc::ProcName& proc_name,
    const void* task_input,
    uint64_t task_input_size,
    ImpersonateSession *session);

  /**
   * @brief A shorthand for impersonating a session and synchronously waiting for its end.
   * @details
   * Useful for a single and synchronous task invocation.
   * This is equivalent to the following impersonate() invocation.
   * @code{.cpp}
   * ImpersonateSession session;
   * if (!pool.impersonate(proc_name, task_input, task_input_size, &session)) {
   *   return ERROR_STACK(kErrorCodeThrNoThreadAvailable);
   * }
   * return session.get_result();
   * @endcode{.cpp}
   * @return Error code of the impersonation or (if impersonation succeeds) of the task.
   * This returns kRetOk iff impersonation and the task succeed.
   */
  ErrorStack impersonate_synchronous(
    const proc::ProcName& proc_name,
    const void* task_input = CXX11_NULLPTR,
    uint64_t task_input_size = 0) {
    ImpersonateSession session;
    if (!impersonate(proc_name, task_input, task_input_size, &session)) {
      return ERROR_STACK(kErrorCodeThrNoThreadAvailable);
    }
    return session.get_result();
  }

  /**
   * Overload to specify a NUMA node to run on.
   * @see impersonate()
   */
  bool impersonate_on_numa_node(
    ThreadGroupId node,
    const proc::ProcName& proc_name,
    const void* task_input,
    uint64_t task_input_size,
    ImpersonateSession *session);

  /**
   * @brief A shorthand for impersonating a session and synchronously waiting for its end.
   * @see impersonate_synchronous()
   */
  ErrorStack impersonate_on_numa_node_synchronous(
    ThreadGroupId node,
    const proc::ProcName& proc_name,
    const void* task_input = CXX11_NULLPTR,
    uint64_t task_input_size = 0) {
    ImpersonateSession session;
    if (!impersonate_on_numa_node(node, proc_name, task_input, task_input_size, &session)) {
      return ERROR_STACK(kErrorCodeThrNoThreadAvailable);
    }
    return session.get_result();
  }

  /**
   * Overload to specify a core to run on.
   * @see impersonate()
   */
  bool impersonate_on_numa_core(
    ThreadId core,
    const proc::ProcName& proc_name,
    const void* task_input,
    uint64_t task_input_size,
    ImpersonateSession *session);

  /**
   * @brief A shorthand for impersonating a session and synchronously waiting for its end.
   * @see impersonate_synchronous()
   */
  ErrorStack impersonate_on_numa_core_synchronous(
    ThreadId core,
    const proc::ProcName& proc_name,
    const void* task_input = CXX11_NULLPTR,
    uint64_t task_input_size = 0) {
    ImpersonateSession session;
    if (!impersonate_on_numa_core(core, proc_name, task_input, task_input_size, &session)) {
      return ERROR_STACK(kErrorCodeThrNoThreadAvailable);
    }
    return session.get_result();
  }

  /** Returns the pimpl of this object. Use it only when you know what you are doing. */
  ThreadPoolPimpl*    get_pimpl() const { return pimpl_; }

  ThreadGroupRef*     get_group_ref(ThreadGroupId numa_node);
  ThreadRef*          get_thread_ref(ThreadId id);

  friend  std::ostream& operator<<(std::ostream& o, const ThreadPool& v);

 private:
  ThreadPoolPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_HPP_
