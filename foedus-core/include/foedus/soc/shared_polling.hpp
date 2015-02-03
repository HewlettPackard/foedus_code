/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SHARED_POLLING_HPP_
#define FOEDUS_SOC_SHARED_POLLING_HPP_

#include <stdint.h>

#include "foedus/cxx11.hpp"

namespace foedus {
namespace soc {

/** Default value of polling_spins */
const uint64_t kDefaultPollingSpins = (1ULL << 16);
/** Default value of max_interval_us */
const uint64_t kDefaultPollingMaxIntervalUs = (1ULL << 15);
/** Initial value of sleep interval in us */
const uint64_t kInitialPollingIntervalUs = (1ULL << 8);

/**
 * @brief A polling-wait mechanism that can be placed in shared memory and used from multiple
 * processes.
 * @ingroup SOC
 * @details
 * This class automatically switches from spinning to polling with sleeps, then exponentially
 * increases the sleep interval with some upper limit.
 * Usually, a condition-variable packages such a functionality.
 * \b BUT, we got so many troubles with glibc's bugs. A fix was pushed to upstream, but
 * there will be many environments that still have older glibc.
 * This class is pthread-free. No glibc versioning issue.
 * Also, does not need signaling functionality in underlying OS/hardware, which we might not
 * have in The Machine.
 *
 * A typical usage is:
 * @code{.cpp}
 * // waiter-side
 * while (true) {
 *   uint64_t demand = polling.acquire_ticket();
 *   // the check AFTER acquiring the ticket is required to avoid lost signal
 *   if (real_condition_is_met) break;
 *   polling.wait(demand);
 * }
 * // signaller-side
 * set_real_condition();  // set the real condition BEFORE signal. otherwise lost signal possible.
 * polling.signal();
 * @endcode
 */
class SharedPolling CXX11_FINAL {
 public:
  SharedPolling() { initialize(); }

  // Disable copy constructors
  SharedPolling(const SharedPolling&) CXX11_FUNC_DELETE;
  SharedPolling& operator=(const SharedPolling&) CXX11_FUNC_DELETE;

  void initialize();

  /**
   * Gives the ticket to.
   * This method takes an acquire fence after getting the ticket.
   * The waiter should check the real condition variable after calling this method
   * to avoid lost signal.
   */
  uint64_t  acquire_ticket() const;

  /**
   * Unconditionally wait for signal.
   * @param[in] demanded_ticket returns when cur_ticket_ becomes this value or larger.
   * @param[in] polling_spins we stop spinning and switch to sleeps after this number of spins
   * @param[in] max_interval_us the sleep interval exponentially grows up to this value in microsec.
   */
  void wait(
    uint64_t demanded_ticket,
    uint64_t polling_spins = kDefaultPollingSpins,
    uint64_t max_interval_us = kDefaultPollingMaxIntervalUs) const;

  /**
   * Wait for signal up to the given timeout.
   * @param[in] demanded_ticket returns when cur_ticket_ becomes this value or larger.
   * @param[in] timeout_microsec timeout in microsec
   * @param[in] polling_spins we stop spinning and switch to sleeps after this number of spins
   * @param[in] max_interval_us the sleep interval exponentially grows up to this value in microsec.
   * @return whether this thread received a signal
   */
  bool timedwait(
    uint64_t demanded_ticket,
    uint64_t timeout_microsec,
    uint64_t polling_spins = kDefaultPollingSpins,
    uint64_t max_interval_us = kDefaultPollingMaxIntervalUs) const;

  /**
   * Signal it to let waiters exit.
   * This method first takes a release fence to avoid lost signal.
   * The signaller should set the real condition variable before calling this method
   * to avoid lost signal.
   */
  void signal();

 private:
  /**
   * Represent how many times it was signalled.
   * Waiter waits on this variable.
   */
  uint64_t  cur_ticket_;

  void spin_poll(uint64_t demanded_ticket, uint64_t polling_spins) const;
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_POLLING_HPP_
