/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/shared_polling.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"

namespace foedus {
namespace soc {

void SharedPolling::initialize() {
  cur_ticket_ = 0;
  assorted::memory_fence_acq_rel();
}

void ugly_atomic_inc(uint64_t* address) {
  reinterpret_cast< std::atomic<uint64_t>* >(address)->operator++();
}


void SharedPolling::wait(
  uint64_t demanded_ticket,
  uint64_t polling_spins,
  uint64_t max_interval_us) const {
  if (cur_ticket_ >= demanded_ticket) {
    return;
  }
  spin_poll(demanded_ticket, polling_spins);

  uint64_t interval_us = kInitialPollingIntervalUs;
  while (cur_ticket_ < demanded_ticket) {
    std::this_thread::sleep_for(std::chrono::microseconds(interval_us));
    interval_us = std::min<uint64_t>(interval_us * 2ULL, max_interval_us);
  }
}

uint64_t get_now_microsec() {
  std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
}

bool SharedPolling::timedwait(
  uint64_t demanded_ticket,
  uint64_t timeout_microsec,
  uint64_t polling_spins,
  uint64_t max_interval_us) const {
  if (cur_ticket_ >= demanded_ticket) {
    return true;
  }
  uint64_t start_us = get_now_microsec();
  uint64_t end_us = start_us + timeout_microsec;  // might overflow as a rare case, but not an issue
  spin_poll(demanded_ticket, polling_spins);

  uint64_t interval_us = kInitialPollingIntervalUs;
  while (cur_ticket_ < demanded_ticket) {
    uint64_t now_us = get_now_microsec();
    if (now_us > end_us) {
      return false;  // ah, oh, timeout
    }
    std::this_thread::sleep_for(std::chrono::microseconds(interval_us));
    interval_us = std::min<uint64_t>(interval_us * 2ULL, max_interval_us);
  }
  return true;
}

void SharedPolling::spin_poll(uint64_t demanded_ticket, uint64_t polling_spins) const {
  for (uint64_t i = 0; i < polling_spins; ++i) {
    if (cur_ticket_ >= demanded_ticket) {
      return;
    }
    assorted::spinlock_yield();
    assorted::memory_fence_acquire();
  }
}


void SharedPolling::signal() {
  assorted::memory_fence_acq_rel();  // well, atomic op implies a full barrier, but to make sure.
  ugly_atomic_inc(&cur_ticket_);
}

uint64_t SharedPolling::acquire_ticket() const {
  uint64_t ret = cur_ticket_ + 1ULL;
  assorted::memory_fence_acq_rel();
  return ret;
}

}  // namespace soc
}  // namespace foedus
