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
#include "foedus/soc/shared_mutex.hpp"

#include <errno.h>
#include <time.h>
#include <sys/time.h>

#include "foedus/assert_nd.hpp"

namespace foedus {
namespace soc {

void SharedMutex::initialize(bool recursive) {
  uninitialize();
  int attr_ret = ::pthread_mutexattr_init(&attr_);
  ASSERT_ND(attr_ret == 0);

  int shared_ret = ::pthread_mutexattr_setpshared(&attr_, PTHREAD_PROCESS_SHARED);
  ASSERT_ND(shared_ret == 0);

  int type_ret = ::pthread_mutexattr_settype(
    &attr_,
    recursive ? PTHREAD_MUTEX_RECURSIVE_NP : PTHREAD_MUTEX_FAST_NP);
  ASSERT_ND(type_ret == 0);

  int mutex_ret = ::pthread_mutex_init(&mutex_, &attr_);
  ASSERT_ND(mutex_ret == 0);

  recursive_ = recursive;
  initialized_ = true;
}

void SharedMutex::uninitialize() {
  if (!initialized_) {
    return;
  }

  int mutex_ret = ::pthread_mutex_destroy(&mutex_);
  ASSERT_ND(mutex_ret == 0);

  int attr_ret = ::pthread_mutexattr_destroy(&attr_);
  ASSERT_ND(attr_ret == 0);

  initialized_ = false;
}

void SharedMutex::lock() {
  ASSERT_ND(initialized_);
  int ret = ::pthread_mutex_lock(&mutex_);
  ASSERT_ND(ret == 0);
}

bool SharedMutex::timedlock(uint64_t timeout_nanosec) {
  if (timeout_nanosec == 0) {
    return trylock();
  }

  ASSERT_ND(initialized_);
  struct timespec timeout;
  struct timeval now;
  ::gettimeofday(&now, CXX11_NULLPTR);
  timeout.tv_sec = now.tv_sec + (timeout_nanosec / 1000000000ULL);
  timeout.tv_nsec = now.tv_usec * 1000ULL + timeout_nanosec % 1000000000ULL;
  timeout.tv_sec += (timeout.tv_nsec) / 1000000000ULL;
  timeout.tv_nsec %= 1000000000ULL;
  int ret = ::pthread_mutex_timedlock(&mutex_, &timeout);
  ASSERT_ND(ret == 0 || ret == ETIMEDOUT);
  return ret == 0;
}

bool SharedMutex::trylock() {
  ASSERT_ND(initialized_);
  int ret = ::pthread_mutex_trylock(&mutex_);
  return ret == 0;
}

void SharedMutex::unlock() {
  ASSERT_ND(initialized_);
  int ret = ::pthread_mutex_unlock(&mutex_);
  ASSERT_ND(ret == 0);
}

void SharedMutexScope::lock() {
  if (locked_by_me_) {
    return;
  }

  mutex_->lock();
  locked_by_me_ = true;
}

void SharedMutexScope::unlock() {
  if (!locked_by_me_) {
    return;
  }

  mutex_->unlock();
  locked_by_me_ = false;
}

}  // namespace soc
}  // namespace foedus
