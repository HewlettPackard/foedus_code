/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/shared_cond.hpp"

#include <errno.h>
#include <time.h>
#include <sys/time.h>

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

void SharedCond::wait(SharedMutexScope* scope) {
  common_assert(scope);

  assorted::memory_fence_acq_rel();
  ++waiters_;

  assorted::memory_fence_acq_rel();
  // probably pthread_cond_wait implies a full fence, but to make sure.
  int ret = ::pthread_cond_wait(&cond_, mutex_.get_raw_mutex());
  ASSERT_ND(ret == 0);
  assorted::memory_fence_acq_rel();

  ASSERT_ND(waiters_ > 0);
  --waiters_;
  assorted::memory_fence_acq_rel();
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

  assorted::memory_fence_acq_rel();
  ++waiters_;

  assorted::memory_fence_acq_rel();
  int ret = ::pthread_cond_timedwait(&cond_, mutex_.get_raw_mutex(), &timeout);
  ASSERT_ND(ret == 0 || ret == ETIMEDOUT);
  assorted::memory_fence_acq_rel();

  ASSERT_ND(waiters_ > 0);
  --waiters_;
  assorted::memory_fence_acq_rel();
  return ret == 0;
}

void SharedCond::broadcast(SharedMutexScope* scope) {
  common_assert(scope);

  assorted::memory_fence_acq_rel();
  ++notifiers_;
  assorted::memory_fence_acq_rel();

  // to avoid the glibc 2.18 pthread bug in broadcast, we use signal, one by one.
  scope->unlock();
  while (waiters_ > 0) {
    int ret = ::pthread_cond_signal(&cond_);
    ASSERT_ND(ret == 0);
    assorted::memory_fence_acq_rel();
    assorted::spinlock_yield();
  }

  assorted::memory_fence_acq_rel();
  --notifiers_;
  assorted::memory_fence_acq_rel();
}

void SharedCond::signal(SharedMutexScope* scope) {
  common_assert(scope);

  assorted::memory_fence_acq_rel();
  ++notifiers_;
  assorted::memory_fence_acq_rel();

  scope->unlock();
  if (waiters_ > 0) {
    int ret = ::pthread_cond_signal(&cond_);
    ASSERT_ND(ret == 0);
  }

  assorted::memory_fence_acq_rel();
  --notifiers_;
  assorted::memory_fence_acq_rel();
}

}  // namespace soc
}  // namespace foedus
