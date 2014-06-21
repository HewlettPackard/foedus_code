/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_IMPERSONATE_TASK_HPP_
#define FOEDUS_THREAD_IMPERSONATE_TASK_HPP_
#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/thread/fwd.hpp"
namespace foedus {
namespace thread {
/**
 * @brief Base class of a task to be executed when impersonation succedes.
 * @ingroup THREADPOOL
 * @details
 * The user program should define a class that derives from this class and pass it to
 * ThreadPool#impersonate(). Each task might be fine-grained (eg. one short transaction as a task)
 * or coarse-graiend (eg. a series of transactions assigned to one thread/CPU-core, like DORA).
 * This interface is totally general, so it's up to the client.
 *
 * @attention One common mistake is to destroy the task object before the completion of the task.
 * You will give a pointer to this object when you impersonate. Do \b not destroy the task object
 * until the task exits! For example:
 * @code{.cpp}
 * // Example of misuse. Destroying a task object too early.
 * std::vector<ImpersonateSession> sessions;
 * for (int i = 0; i < 10; ++i) {
 *     MyTask task;  // WRONG! You are destroing the task object too early.
 *     sessions.push_back(engine.get_thread_pool().impersonate(&task));
 * }
 * for (int i = 0; i < 10; ++i) {
 *     if (session.is_valid()) {
 *         std::cout << "result[" << i << "]=" << session.get_result() << std::endl;
 *     } else {
 *         std::cout << "session[" << i << "] didn't start=" << session.invalid_cause_ << std::endl;
 *     }
 * }
 * @endcode
 * @code{.cpp}
 * // Corrected version.
 * std::vector<MyTask*> tasks;  // Notice that it's vector of MyTask*, not of MyTask
 * std::vector<ImpersonateSession> sessions;
 * for (int i = 0; i < 10; ++i) {
 *     tasks.push_back(new MyTask());
 *     sessions.push_back(engine.get_thread_pool().impersonate(tasks.back()));
 * }
 * for (int i = 0; i < 10; ++i) {
 *     if (session.is_valid()) {
 *         std::cout << "result[" << i << "]=" << session.get_result() << std::endl;
 *     } else {
 *         std::cout << "session[" << i << "] didn't start=" << session.invalid_cause_ << std::endl;
 *     }
 *     delete tasks[i];  // we delete the task after session.get_result().
 * }
 * @endcode
 *
 * An equivalent mistake can be made via object copies.
 * Hence, to prevent misuse, copy/assignment are disabled in this class (and should be in derived
 * classes, too).
 */
class ImpersonateTask {
 public:
  friend class ImpersonateSession;
  friend class ThreadPimpl;

  // non-copyable, non-assignable
  ImpersonateTask(const ImpersonateTask &other) CXX11_FUNC_DELETE;
  ImpersonateTask& operator=(const ImpersonateTask &other) CXX11_FUNC_DELETE;

  ImpersonateTask();
  virtual ~ImpersonateTask();

  /**
   * @brief The method called back from the engine on one of its pooled threads.
   * @param[in] context the impersonated thread
   * @details
   * Note that the thread calling this method is NOT the client program's thread
   * that invoked ThreadPool#impersonate(), but an asynchronous thread that was pre-allocated
   * in the engine.
   * When this method returns, the engine releases the impersonated thread to the thread pool
   * so that other clients can grab it for new sessions.
   */
  virtual ErrorStack run(Thread* context) = 0;

 private:
  /** pimpl of this object. */
  ImpersonateTaskPimpl*   pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_IMPERSONATE_TASK_HPP_
