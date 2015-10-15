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
#ifndef FOEDUS_SSSP_SSSP_CLIENT_CHIME_HPP_
#define FOEDUS_SSSP_SSSP_CLIENT_CHIME_HPP_

#include <stdint.h>

#include <atomic>
#include <memory>
#include <thread>

namespace foedus {
namespace sssp {

/**
 * 0 means null. see below.
 */
typedef uint32_t AnalyticEpoch;
typedef std::atomic<AnalyticEpoch>* AnalyticEpochPtr;

const AnalyticEpoch kNullAnalyticEpoch = 0;

/**
 * @brief a background thread to keep checking the end of an analytic query.
 * @details
 * This background thread is launched by the analytic leader.
 * It periodically checks everyone's loop counter. When it's sure that
 * all analytic workers are waiting for a task, it sets query_ended_ to true.
 *
 * @par Detailed Algorithm
 * Simplest way to detect the end of a query is to do a heavy-weight counting or
 * other synchronization whenever a worker receives/completes a task, but
 * we don't want that. Instead, we do the following.
 *
 *   \li This chime periodically announces "epoch", every 100 microsec or something.
 * (don't be confused with the epoch in SILO protocol, this one is much more fine grained)
 *
 *   \li All workers, whenever a loop didn't find any update in their L1 counters,
 * copy the global epoch to two counters; \e clean_since and \e clean_upto.
 * They do not update \e clean_since when they haven't received any update since
 * the last time. Whenever they found an update, they change both of them back to 0.
 *
 *   \li Any \e E such that clean_since < E < clean_upto is guaranteed to be their
 * \e clean-period of the worker. During clean-period, the worker is guaranteed to
 * 1) have received no update, and 2) have sent no update to others.
 * For example, clean_since=3 and clean_upto=6 mean
 * clean-period is 4 and 5. Note that 3 and 6 are \b not part of the clean-period
 * because we don't know exactly when the worker got in/out of the clean-state.
 *
 *   \li The chime thread also periodically checks clean-period of all workers.
 * If all workers have non-0 clean-periods and there is some full overlap (can easily
 * check with min/max), that means at least at some point, everyone has not received
 * any task or sent any task to others. As of that point and onwards, there is
 * no chance that the query does anything.
 */
class SsspAnalyticChime {
 public:
  void start_chime(
    AnalyticEpochPtr analytic_chime_epoch_address,
    AnalyticEpochPtr* target_clean_since_addresses,
    AnalyticEpochPtr* target_clean_upto_addresses,
    uint32_t target_count,
    std::atomic<bool>* query_ended_address);
  void stop_chime();

 private:
  AnalyticEpochPtr analytic_chime_epoch_address_;
  std::unique_ptr<AnalyticEpochPtr[]> target_clean_since_addresses_;
  std::unique_ptr<AnalyticEpochPtr[]> target_clean_upto_addresses_;

  uint32_t            target_count_;
  std::atomic<bool>*  query_ended_address_;
  std::atomic<bool>   stop_requested_;
  std::thread         chime_thread_;

  void chime_handler();
  bool chime_check();
};

}  // namespace sssp
}  // namespace foedus

#endif  // FOEDUS_SSSP_SSSP_CLIENT_CHIME_HPP_
