/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_POOL_HPP_
#define FOEDUS_THREAD_THREAD_POOL_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/impersonate_session.hpp>
#include <foedus/thread/impersonate_task.hpp>
#include <stdint.h>
#include <iosfwd>
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
 *  \li \b Task (ImpersonateTask) is an arbitrary user-defined code to run in our engine.
 *  \li \b Session (ImpersonateSession) is a one-to-one mapping between a task and a pooled thread
 * that continues until the completion of the task.
 *  \li \b Impersonation (ThreadPool#impersonate()) is an action to create a session for the
 * given task.
 *
 * In order to start a user transaction or a series of user transactions on some thread,
 * the user first defines the task as a class that derives from ImpersonateTask.
 * Then, the user calls ThreadPool#impersonate() to impersonate one of the pre-allocated
 * threads for the task.
 *
 * When the thread is impersonated, it starts runninng the task asynchronously.
 * The original client thread, which invoked the impersonation, immediately returns
 * with ImpersonateSession object for the acquired session.
 * The original thread can choose to either synchronously wait for the completion of the task or
 * do other stuffs, such as launching more sessions.
 *
 * This means that a use code doesn't have to do \b anything to run the tasks on an arbitrary
 * number of threads. Everything is encapsulated in the ThreadPool class and its related classes.
 *
 * @section EX Examples
 * Below is a trivial example to define a task and submit impersonation request.
 * @code{.cpp}
 * #include <foedus/engine.hpp>
 * #include <foedus/engine_options.hpp>
 * #include <foedus/thread/thread_pool.hpp>
 * #include <foedus/thread/thread.hpp>
 *
 * class MyTask : public foedus::thread::ImpersonateTask {
 * public:
 *     foedus::ErrorStack run(foedus::thread::Thread* context) {
 *         std::cout << "Ya!" << std::endl;
 *         return foedus::kRetOk;
 *     }
 * };
 *
 * int main(int argc, char **argv) {
 *     foedus::EngineOptions options;
 *     foedus::Engine engine(options);
 *     if (engine.initialize().is_error()) {
 *         return 1;
 *     }
 *
 *     foedus::UninitializeGuard guard(&engine);
 *     MyTask task;
 *     foedus::thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
 *     if (session.is_valid()) {
 *         std::cout << "result=" << session.get_result() << std::endl;
 *     } else {
 *         std::cout << "session didn't start=" << session.invalid_cause_ << std::endl;
 *     }
 *     if (engine.uninitialize().is_error()) {
 *         return 1;
 *     }
 *
 *     return 0;
 * }
 * @endcode
 */

/**
 * @brief The pool of pre-allocated threads in the engine to execute transactions.
 * @ingroup THREADPOOL
 * @details
 * This is the main API class of thread package.
 * Its gut is impersonate() which allows client programs to create a new session
 * (ImpersonateSession) that runs user-defined functions (ImpersonateTask).
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
     * @brief Impersonate as one of pre-allocated threads in this engine, calling
     * back the functor from the impersonated thread (\b NOT the current thread).
     * @param[in] task the callback functor the client program should define. The pointer
     * must be valid at least until the completion of the session.
     * @param[in] timeout how long we wait for impersonation if there is no available thread
     * @details
     * This is similar to launch a new thread that calls the functor.
     * The difference is that this doesn't actually create a thread (which is very expensive)
     * but instead just impersonates as one of the pre-allocated threads in the engine.
     * @return The resulting session.
     * @todo currently, timeout is ignored. It behaves as if timeout=0
     */
    ImpersonateSession  impersonate(ImpersonateTask* task, TimeoutMicrosec timeout = -1);

    /**
     * @brief A shorthand for impersonating a session and synchronously waiting for its end.
     * @details
     * Useful for a single and synchronous task invocation.
     * This is equivalent to the following impersonate() invocation.
     * @code{.cpp}
     * ImpersonateSession session = pool.impersonate(task);
     * if (!session.is_valid()) {
     *   return session.invalid_cause_;
     * }
     * return session.get_result();
     * @endcode{.cpp}
     * @return Error code of the impersonation or (if impersonation succeeds) of the task.
     * This returns kRetOk iff impersonation and the task succeed.
     */
    ErrorStack          impersonate_synchronous(ImpersonateTask* task) {
        ImpersonateSession session = impersonate(task);
        if (!session.is_valid()) {
            return session.invalid_cause_;
        }
        return session.get_result();
    }

    /**
     * Overload to specify a NUMA node to run on.
     * @see impersonate()
     * @todo currently, timeout is ignored. It behaves as if timeout=0
     */
    ImpersonateSession  impersonate_on_numa_node(ImpersonateTask* task,
                                        ThreadGroupId numa_node, TimeoutMicrosec timeout = -1);

    /**
     * Overload to specify a core to run on.
     * @see impersonate()
     * @todo currently, timeout is ignored. It behaves as if timeout=0
     */
    ImpersonateSession  impersonate_on_numa_core(ImpersonateTask* task,
                                        ThreadId numa_core, TimeoutMicrosec timeout = -1);

    /** Returns the pimpl of this object. Use it only when you know what you are doing. */
    ThreadPoolPimpl*    get_pimpl() const { return pimpl_; }

    friend  std::ostream& operator<<(std::ostream& o, const ThreadPool& v);

 private:
    ThreadPoolPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_HPP_
