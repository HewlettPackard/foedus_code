/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_CONDITION_VARIABLE_IMPL_HPP_
#define FOEDUS_THREAD_CONDITION_VARIABLE_IMPL_HPP_
#include <stdint.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace thread {
/**
 * @brief An analogue of pthread's condition variable and std::condition_variable to avoid glibc's
 * bug in pthread_cond_broadcast/signal (thus in notify_all/one in turn).
 * @ingroup THREAD
 * @details
 * The whole purpose of this class is to workaound a bug in glibc's pthread_cond_broadcast/signal(),
 * which is also used by std::condition_variable.
 *
 * The bug is already fixed here:
 *   https://sourceware.org/git/?p=glibc.git;a=commitdiff;h=8f630cca5c36941db1cb48726016bbed80ec1041
 * However, it will not be available until glibc 2.20, which will be employed in major linux distros
 * much much later. So, we work around the issue ourselves for a while.
 *
 * Seems like the issue is not only about broadcast. I'm observing issues if we might call
 * notify_one while there is no waiter (which should be totally valid, but then something gets
 * occasionally broken.) So, whether you have at most one waiter not, we should use this class
 * always. Do not use std::condition_variable at all.
 *
 * As this depends on C++11, the name of this file ends with impl. Thus, only private implementation
 * classes directly use this class. If you are okay with C++11, you can use it from client programs,
 * too.
 *
 * This class is totally header-only.
 */
class ConditionVariable final {
 public:
  ConditionVariable() : waiters_(0), notifiers_(0) {}
  ~ConditionVariable() {
    ASSERT_ND(waiters_ == 0);
    // we must wait until all notifiers exit notify_all.
    // this assumes no new notifiers are newly coming in this situation.
    while (notifiers_ > 0) {
      assorted::spinlock_yield();
    }
  }

  // not copyable, assignable.
  ConditionVariable(const ConditionVariable &other) = delete;
  ConditionVariable& operator=(const ConditionVariable &other) = delete;
  ConditionVariable(ConditionVariable &&other) = delete;
  ConditionVariable& operator=(ConditionVariable &&other) = delete;

  /**
   * @brief Block until the event happens.
   * @details
   * Equivalent to std::condition_variable::wait().
   */
  template<typename PREDICATE>
  void wait(PREDICATE predicate) {
    WaiterScope scope(this);
    condition_.wait(scope.lock_, predicate);
  }
  /**
   * @brief Block until the event \b PROBABLY (because this version is w/o pred) happens.
   * @details
   * Equivalent to std::condition_variable::wait().
   */
  void wait() {
    WaiterScope scope(this);
    condition_.wait(scope.lock_);
  }

  /**
   * @brief Block until the event happens \b or the given period elapses.
   * @return whether the event happened by now.
   * @details
   * Equivalent to std::condition_variable::wait_for().
   */
  template<class REP, class PERIOD, typename PREDICATE>
  bool wait_for(const std::chrono::duration<REP, PERIOD>& timeout, PREDICATE predicate) {
    WaiterScope scope(this);
    return condition_.wait_for(scope.lock_, timeout, predicate);
  }

  /**
   * @brief Block until the event happens \b or the given period elapses.
   * @return whether the event \b PROBABLY (because this version is w/o pred) happened by now.
   * @details
   * Equivalent to std::condition_variable::wait_for().
   */
  template<class REP, class PERIOD>
  bool wait_for(const std::chrono::duration<REP, PERIOD>& timeout) {
    WaiterScope scope(this);
    return condition_.wait_for(scope.lock_, timeout) == std::cv_status::no_timeout;
  }

  /**
   * @brief Block until the event happens \b or the given time point arrives.
   * @return whether the event happened by now.
   * @details
   * Equivalent to std::condition_variable::wait_until().
   */
  template< class CLOCK, class DURATION, typename PREDICATE>
  bool wait_until(const std::chrono::time_point<CLOCK, DURATION>& until, PREDICATE predicate) {
    WaiterScope scope(this);
    return condition_.wait_until(scope.lock_, until, predicate);
  }

  /**
   * @brief Block until the event happens \b or the given time point arrives.
   * @return whether the event \b PROBABLY (because this version is w/o pred) happened by now.
   * @details
   * Equivalent to std::condition_variable::wait_until().
   */
  template< class CLOCK, class DURATION>
  bool wait_until(const std::chrono::time_point<CLOCK, DURATION>& until) {
    WaiterScope scope(this);
    return condition_.wait_until(scope.lock_, until) == std::cv_status::no_timeout;
  }

  /**
   * @brief Notify all waiters that the event has happened.
   * @param[in] signal_action Functor to update actual values that changes the condition variable
   * to signaling state. This will be executed in critical section to avoid lost signal and
   * spurious wakeup.
   * @details
   * Equivalent to std::condition_variable::notify_all().
   * To workaround the pthread_cond_broadcast bug, this method notifies one by one.
   * We might add a switch of the behavior by checking glibc version.
   */
  template<typename SIGNAL_ACTION>
  void notify_all(SIGNAL_ACTION signal_action) {
    NotifierScope scope(this);
    {
      std::lock_guard<std::mutex> guard(mutex_);
      signal_action();  // conduct the action to update actual values *in* critical section
    }
    while (waiters_ > 0) {
      condition_.notify_one();
      assorted::spinlock_yield();
    }
  }
  /**
   * @brief Notify all waiters that the event has happened.
   * @details
   * Equivalent to std::condition_variable::notify_all().
   * To workaround the pthread_cond_broadcast bug, this method notifies one by one.
   * We might add a switch of the behavior by checking glibc version.
   */
  void notify_all() {
    notify_all([]{});
  }

  /**
   * @brief Notify one waiter that the event has happened.
   * @param[in] signal_action Functor to update actual values that changes the condition variable
   * to signaling state. This will be executed in critical section to avoid lost signal and
   * spurious wakeup.
   * @details
   * Equivalent to std::condition_variable::notify_one().
   * To workaround the pthread_cond_signal bug, this method atomically checks if there is any
   * waiter, avoiding to call notify_one() if there is none.
   * We might add a switch of the behavior by checking glibc version.
   */
  template<typename SIGNAL_ACTION>
  void notify_one(SIGNAL_ACTION signal_action) {
    NotifierScope scope(this);
    {
      std::lock_guard<std::mutex> guard(mutex_);
      signal_action();
    }
    if (waiters_ > 0) {
      condition_.notify_one();
    }
  }

  /**
   * @brief Notify one waiter that the event has happened.
   * @details
   * Equivalent to std::condition_variable::notify_one().
   * To workaround the pthread_cond_signal bug, this method atomically checks if there is any
   * waiter, avoiding to call notify_one() if there is none.
   * We might add a switch of the behavior by checking glibc version.
   */
  void notify_one() {
    notify_one([]{});
  }

 private:
  /** used to notify waiters to wakeup. */
  std::condition_variable         condition_;
  /**
   * Protects the condition variable and read/write of actual values.
   * The actual values (wait condition) must be atomically read/written to avoid spurrious
   * wakeup \b and lost signals.
   */
  std::mutex                      mutex_;

  /** Number of waitors. */
  std::atomic<uint32_t>           waiters_;
  /** Number of notifiers, used to safely destruct this object. */
  std::atomic<uint32_t>           notifiers_;

  /** automatically increments/decrements waiter count and does sanity check. */
  struct WaiterScope {
    explicit WaiterScope(ConditionVariable* enclosure)
      : enclosure_(enclosure), lock_(enclosure_->mutex_) {
      ASSERT_ND(lock_.owns_lock());
      ++enclosure_->waiters_;
    }
    ~WaiterScope() {
      ASSERT_ND(lock_.owns_lock());
      ASSERT_ND(enclosure_->waiters_ > 0);
      --enclosure_->waiters_;
    }
    ConditionVariable* const enclosure_;
    std::unique_lock<std::mutex> lock_;
  };
  /** same for notifier. */
  struct NotifierScope {
    explicit NotifierScope(ConditionVariable* enclosure) : enclosure_(enclosure) {
      ++enclosure_->notifiers_;
    }
    ~NotifierScope() {
      ASSERT_ND(enclosure_->notifiers_ > 0);
      --enclosure_->notifiers_;
    }
    ConditionVariable* const enclosure_;
  };
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_CONDITION_VARIABLE_IMPL_HPP_
