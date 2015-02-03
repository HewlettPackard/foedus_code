/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/shared_rendezvous.hpp"

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"

namespace foedus {
namespace soc {

void SharedRendezvous::initialize() {
  uninitialize();
  signaled_ = false;
  initialized_ = true;
  cond_.initialize();
}

void SharedRendezvous::uninitialize() {
  if (!is_initialized()) {
    return;
  }
}

void SharedRendezvous::wait() {
  if (is_signaled_weak()) {  // just an optimization.
    return;
  }

  uint64_t demand = cond_.acquire_ticket();
  if (is_signaled()) {
    return;
  }
  cond_.wait(demand);
  ASSERT_ND(is_signaled());
}

bool SharedRendezvous::wait_for(uint64_t timeout_nanosec) {
  if (is_signaled_weak()) {  // just an optimization.
    return true;
  }

  uint64_t demand = cond_.acquire_ticket();
  if (is_signaled()) {
    return true;
  }
  bool received = cond_.timedwait(demand, timeout_nanosec / 1000);
  ASSERT_ND(!received || is_signaled());
  return is_signaled();
}

void SharedRendezvous::signal() {
  signaled_ = true;
  cond_.signal();
}

}  // namespace soc
}  // namespace foedus
