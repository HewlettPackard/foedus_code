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
#ifndef FOEDUS_SOC_SHARED_COND_HPP_
#define FOEDUS_SOC_SHARED_COND_HPP_

#include <pthread.h>
#include <stdint.h>

#include "foedus/cxx11.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/soc/shared_polling.hpp"

namespace foedus {
namespace soc {
/**
 * @brief A conditional variable that can be placed in shared memory
 * and used from multiple processes.
 * @ingroup SOC
 * @details
 * Analogous to SharedMutex. This is for conditional variable.
 * This class also avoids the pthread bug described in foedus::thread::ConditionVariable.
 * This file does not assume C++11.
 */
class SharedCond CXX11_FINAL {
 public:
  SharedCond() : initialized_(false), waiters_(0), notifiers_(0) { initialize(); }
  ~SharedCond() { uninitialize(); }

  // Disable copy constructors
  SharedCond(const SharedCond&) CXX11_FUNC_DELETE;
  SharedCond& operator=(const SharedCond&) CXX11_FUNC_DELETE;

  void initialize();
  void uninitialize();
  bool is_initialized() const { return initialized_; }

  /**
   * @brief Unconditionally wait for the event.
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is still locked
   * @details
   * This method does \b NOT rule out spurrious wakeup.
   * We could receive a lambda to check the condition, but this class should be C++11-free.
   * So, the caller should do the loop herself if she doesn't want a spurrious wakeup.
   * Instead, you can easily avoid lost signals by checking the condition after locking the mutex
   * before calling this method.
   */
  void wait(SharedMutexScope* scope);

  /**
   * @brief Wait for the event up to the given timeout.
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @param[in] timeout_nanosec timeout in nanoseconds
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is still locked
   * @return whether this thread received the signal (though still spurrious wakeup possible).
   * false if timeout happened.
   * @details
   * This method does \b NOT rule out spurrious wakeup as described above.
   */
  bool timedwait(SharedMutexScope* scope, uint64_t timeout_nanosec);

  /**
   * @brief Unblock all waiters
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is no longer locked
   * @details
   * You should set the real condition variable itself after locking the mutex before
   * calling this method to avoid lost signals.
   * @attention Consider using broadcast_nolock(). We encountered a deadlock bug with
   * a very high contention. We were not sure where the problem is; maybe the glibc's
   * pthread_cond_broadcast() issue, simply our code's bug (lack or duplicated release etc), or a
   * contention that causes repeated wakeup/broadcast loop. But, we did observe that
   * the problem went away with broadcast_nolock().
   * @deprecated see above. But, not yet 100% sure why it happened...
   * We should have a wiki entry to track this issue.
   */
  void broadcast(SharedMutexScope* scope);

  /**
   * @brief Unblock all waiters without a mutex held by the signaller
   * @details
   * You should set the real condition variable itself after locking the mutex,
   * \b AND release it before calling this method.
   * This method does not assume a mutex, thus a lost signal is possible.
   */
  void broadcast_nolock();

  /**
   * @brief Unblock one waiter
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is no longer locked
   * @details
   * You should set the real condition variable itself after locking the mutex before
   * calling this method to avoid lost signals.
   */
  void signal(SharedMutexScope* scope);

  /**
   * @brief Returns the mutex that protects this condition variable.
   * @details
   * You must lock this mutex \b BEFORE you call wait/notify/etc in this class along with
   * checking the real boolean condition itself. Otherwise, you will get lost signals.
   * This is why the methods above receive SharedMutexScope as parameter.
   */
  SharedMutex* get_mutex() { return &mutex_; }

  /**
   * A non-synchronized method to tell \b seemingly whether there is a waiter or not.
   * The caller is responsible for using this method with appropriate fences, retries, etc.
   */
  bool exists_waiters() const { return waiters_ != 0; }

 private:
  /** Whether this mutex is ready for use. We don't tolerate race in initialization. */
  bool                initialized_;

  SharedMutex         mutex_;
  pthread_cond_t      cond_;
  pthread_condattr_t  attr_;
  /** Number of waitors. */
  uint32_t            waiters_;
  /** Number of notifiers, used to safely destruct this object. */
  uint32_t            notifiers_;

  void common_assert(SharedMutexScope* scope);
};

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_COND_HPP_
