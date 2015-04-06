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
#include "foedus/soc/shared_cond.hpp"

#include <errno.h>
#include <time.h>
#include <sys/time.h>

#include <atomic>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"

namespace foedus {
namespace soc {

void SharedCond::initialize() {
  uninitialize();
  waiters_ = 0;
  notifiers_ = 0;
  mutex_.initialize();
  int attr_ret = ::pthread_condattr_init(&attr_);
  ASSERT_ND(attr_ret == 0);

  int shared_ret = ::pthread_condattr_setpshared(&attr_, PTHREAD_PROCESS_SHARED);
  ASSERT_ND(shared_ret == 0);

  int cond_ret = ::pthread_cond_init(&cond_, &attr_);
  ASSERT_ND(cond_ret == 0);

  initialized_ = true;
}

void SharedCond::uninitialize() {
  if (!initialized_) {
    return;
  }

  assorted::memory_fence_acquire();
  ASSERT_ND(waiters_ == 0);
  // we must wait until all notifiers exit notify_all.
  // this assumes no new notifiers are newly coming in this situation.
  while (notifiers_ > 0) {
    assorted::spinlock_yield();
    assorted::memory_fence_acquire();
  }

  mutex_.uninitialize();

  int cond_ret = ::pthread_cond_destroy(&cond_);
  ASSERT_ND(cond_ret == 0);

  int attr_ret = ::pthread_condattr_destroy(&attr_);
  ASSERT_ND(attr_ret == 0);

  initialized_ = false;
}


void SharedCond::common_assert(SharedMutexScope* scope) {
  ASSERT_ND(initialized_);
  ASSERT_ND(scope);
  ASSERT_ND(scope->is_locked_by_me());
  ASSERT_ND(scope->get_mutex() == &mutex_);
}

void ugly_atomic_inc(uint32_t* address) {
  reinterpret_cast< std::atomic<uint32_t>* >(address)->fetch_add(1U);
}
void ugly_atomic_dec(uint32_t* address) {
  reinterpret_cast< std::atomic<uint32_t>* >(address)->fetch_sub(1U);
}

void SharedCond::wait(SharedMutexScope* scope) {
  common_assert(scope);

  ugly_atomic_inc(&waiters_);

  // probably pthread_cond_wait implies a full fence, but to make sure.
  int ret = ::pthread_cond_wait(&cond_, mutex_.get_raw_mutex());
  ASSERT_ND(ret == 0);

  ASSERT_ND(waiters_ > 0);
  ugly_atomic_dec(&waiters_);
}

bool SharedCond::timedwait(SharedMutexScope* scope, uint64_t timeout_nanosec) {
  common_assert(scope);
  struct timespec timeout;
  struct timeval now;
  ::gettimeofday(&now, CXX11_NULLPTR);
  timeout.tv_sec = now.tv_sec + (timeout_nanosec / 1000000000ULL);
  timeout.tv_nsec = now.tv_usec * 1000ULL + timeout_nanosec % 1000000000ULL;
  timeout.tv_sec += (timeout.tv_nsec) / 1000000000ULL;
  timeout.tv_nsec %= 1000000000ULL;

  ugly_atomic_inc(&waiters_);

  int ret = ::pthread_cond_timedwait(&cond_, mutex_.get_raw_mutex(), &timeout);
  ASSERT_ND(ret == 0 || ret == ETIMEDOUT);

  ASSERT_ND(waiters_ > 0);
  ugly_atomic_dec(&waiters_);
  return ret == 0;
}

void SharedCond::broadcast(SharedMutexScope* scope) {
  common_assert(scope);

  ugly_atomic_inc(&notifiers_);

  // to avoid the glibc 2.18 pthread bug in broadcast, we use signal, one by one.
  scope->unlock();
  while (waiters_ > 0) {
    // int ret = ::pthread_cond_broadcast(&cond_);
    int ret = ::pthread_cond_signal(&cond_);
    ASSERT_ND(ret == 0);
    assorted::memory_fence_acq_rel();
    assorted::spinlock_yield();
  }

  ugly_atomic_dec(&notifiers_);
}

void SharedCond::broadcast_nolock() {
  ugly_atomic_inc(&notifiers_);
  // int ret = ::pthread_cond_broadcast(&cond_);
  int ret = ::pthread_cond_signal(&cond_);
  ASSERT_ND(ret == 0);
  ugly_atomic_dec(&notifiers_);
}

void SharedCond::signal(SharedMutexScope* scope) {
  common_assert(scope);

  ugly_atomic_inc(&notifiers_);

  scope->unlock();
  if (waiters_ > 0) {
    int ret = ::pthread_cond_signal(&cond_);
    ASSERT_ND(ret == 0);
  }

  ugly_atomic_dec(&notifiers_);
}

}  // namespace soc
}  // namespace foedus
