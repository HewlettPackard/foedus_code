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
#ifndef FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
#define FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
#include <atomic>
#include <chrono>

#include "foedus/assert_nd.hpp"
#include "foedus/thread/condition_variable_impl.hpp"

namespace foedus {
namespace thread {
/**
 * @brief The frequently appearing triplet of condition_varible, "signal" flag for spurious wakeup,
 * and mutex for a one-time single-producer multiple-consumer event synchronization.
 * @ingroup THREAD
 * @details
 * This is basically equivalent to std::promise/future pair with no parameter.
 * The frequent use case is to synchronize with some event for one producer and many waiters.
 * We did use std::promise/future pair for this purpose, but we encountered a bug
 * in libstdc's implementation of std::promise/future.
 *   https://gcc.gnu.org/bugzilla/show_bug.cgi?id=57440
 *
 * We are not sure when the fix will be made, nor when the fixed version of gcc/libstdc++ will be
 * prevalent to all environments we support. Very unlikely we can't afford to wait for it.
 * Therefore, we roll it our own.
 *
 * As this depends on C++11, the name of this file ends with impl. Thus, only private implementation
 * classes directly use this class. If you are okay with C++11, you can use it from client programs,
 * too.
 *
 * This class is totally header-only.
 */
class Rendezvous final {
 public:
  Rendezvous() : signaled_(false) {}

  // not copyable, assignable.
  Rendezvous(const Rendezvous &other) = delete;
  Rendezvous& operator=(const Rendezvous &other) = delete;
  Rendezvous(Rendezvous &&other) = delete;
  Rendezvous& operator=(Rendezvous &&other) = delete;

  /**
   * @brief Block until the event happens.
   * @details
   * Equivalent to std::future<void>::wait().
   */
  void wait() {
    if (is_signaled()) {
      return;
    }
    condition_.wait([this]{ return is_signaled(); });
  }

  /**
   * @brief Block until the event happens \b or the given period elapses.
   * @return whether the event happened by now.
   * @details
   * Equivalent to std::future<void>::wait_for().
   */
  template<class REP, class PERIOD>
  bool wait_for(const std::chrono::duration<REP, PERIOD>& timeout) {
    if (is_signaled()) {
      return true;
    }
    return condition_.wait_for(timeout, [this]{ return is_signaled(); });
  }

  /**
   * @brief Block until the event happens \b or the given time point arrives.
   * @return whether the event happened by now.
   * @details
   * Equivalent to std::future<void>::wait_until().
   */
  template< class CLOCK, class DURATION >
  bool wait_until(const std::chrono::time_point<CLOCK, DURATION>& until) {
    if (is_signaled()) {
      return true;
    }
    return condition_.wait_until(until, [this]{ return is_signaled(); });
  }

  /**
   * @brief Notify all waiters that the event has happened.
   * @details
   * Equivalent to std::promise<void>::set_value().
   * There must be only one thread that might call this method, and it should call this only once.
   * Otherwise, the behavior is undefined.
   */
  void signal() {
    ASSERT_ND(!is_signaled());
    condition_.notify_all([this]{ signaled_.store(true); });
    // we must not put ANYTHING after this because notified waiters might have already
    // deleted this object. notify_broadcast() guarantees that itself finishes before
    // destruction, but no guarantee after that.
  }

  /** returns whether this thread has stopped (if the thread hasn't started, false too). */
  bool is_signaled() const {
    return signaled_.load();
  }
  /** non-atomic is_signaled(). */
  bool is_signaled_weak() const {
    return signaled_.load(std::memory_order_relaxed);
  }

 private:
  /** used to notify waiters to wakeup. */
  ConditionVariable               condition_;
  /** whether this thread has stopped (if the thread hasn't started, false too). */
  std::atomic<bool>               signaled_;
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
