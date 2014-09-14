/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/shared_rendezvous.hpp"

#include <time.h>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"

namespace foedus {
namespace soc {

void SharedRendezvous::initialize() {
  uninitialize();
  signaled_ = false;
  cond_.initialize();
}

void SharedRendezvous::uninitialize() {
  if (!is_initialized()) {
    return;
  }

  cond_.uninitialize();
}

void SharedRendezvous::wait() {
  if (is_signaled_weak()) {  // just an optimization.
    return;
  }

  while (true) {
    SharedMutexScope scope(cond_.get_mutex());
    if (is_signaled()) {  // check _in_ the mutex scope
      break;
    }
    cond_.wait(&scope);
  }
}

bool SharedRendezvous::wait_for(uint64_t timeout_nanosec) {
  if (is_signaled_weak()) {  // just an optimization.
    return true;
  }

  while (true) {
    struct timespec prev, next;
    int clock_ret = ::clock_gettime(CLOCK_REALTIME, &prev);
    ASSERT_ND(clock_ret == 0);

    SharedMutexScope scope(cond_.get_mutex());
    if (is_signaled()) {  // check _in_ the mutex scope
      return true;
    }
    cond_.timedwait(&scope, timeout_nanosec);
    // return value of timedwait doesn't matter.
    // we have to anyway deal with spurrious wakeup.
    if (is_signaled()) {
      return true;
    }
    clock_ret = ::clock_gettime(CLOCK_REALTIME, &next);
    ASSERT_ND(clock_ret == 0);
    uint64_t elapsed_nanosec = (next.tv_sec - prev.tv_sec) * 1000000000ULL
      - (next.tv_nsec - prev.tv_nsec);
    if (elapsed_nanosec >= timeout_nanosec) {
      return false;  // timeout
    } else {
      timeout_nanosec -= elapsed_nanosec;
    }
  }
}

void SharedRendezvous::signal() {
  SharedMutexScope scope(cond_.get_mutex());
  signaled_ = true;  // set _in_ the mutex scope
  cond_.broadcast(&scope);
}

}  // namespace soc
}  // namespace foedus
