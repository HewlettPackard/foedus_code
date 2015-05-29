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
#include <valgrind.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/soc/shared_polling.hpp"

namespace foedus {
namespace soc {

DEFINE_TEST_CASE_PACKAGE(SharedPollingTest, foedus.soc);

// These testcases use only local-memory. but there shouldn't be anything specific to
// shared memory as this class uses no pthread.

TEST(SharedPollingTest, Alone) {
  SharedPolling polling;
  polling.signal();
  uint64_t demand = polling.acquire_ticket();
  EXPECT_GT(demand, 0);  // because after signal
  polling.wait(demand - 1U);  // immediately exit because of "-1"
  polling.signal();
}

const uint64_t kReps = 1U << 12;
struct Data {
  SharedPolling polling_;
  /**
   * Start from zero, and the thread whose id is (data%threads) is responsible to increment it,
   * then signal the polling_.
   */
  uint64_t data_;
};

void run_thread(Data* data, uint32_t thread_id, uint32_t threads) {
  for (uint64_t i = 0; i < kReps; ++i) {
    while (true) {
      uint64_t demand = data->polling_.acquire_ticket();
      if ((data->data_ % threads) == thread_id) {
        break;
      }
      data->polling_.wait(demand);
    }
    EXPECT_EQ(thread_id, data->data_ % threads);
    EXPECT_EQ(thread_id + threads * i, data->data_);
    ++data->data_;
    data->polling_.signal();
  }
}

void test_multi(uint32_t threads) {
  Data data;
  data.data_ = 0;
  std::vector< std::thread > th;
  for (uint32_t i = 0; i < threads; ++i) {
    th.emplace_back(run_thread, &data, i, threads);
  }
  for (auto& t : th) {
    t.join();
  }
  EXPECT_EQ(kReps * threads, data.data_);
}

TEST(SharedPollingTest, OneThread) { test_multi(1); }
TEST(SharedPollingTest, TwoThreads) { test_multi(2); }
TEST(SharedPollingTest, FourThreads) { test_multi(4); }

void run_thread_hold_longtime(SharedPolling* polling) {
  // NOTE: Although 1-sec wait seems too long, this is required for valgrind version of tests.
  // On valgrind, the initial spin part might take long time.
  // Thus, we had a valgrind test-failure with 100ms wait. Fixes #7.
  uint64_t longtime_ms = 100;
  if (RUNNING_ON_VALGRIND) {
    longtime_ms = 1000;  //
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(longtime_ms));
  polling->signal();
}

TEST(SharedPollingTest, Timeout) {
  SharedPolling polling;
  std::thread t(run_thread_hold_longtime, &polling);
  // at least the first one is surely timeout
  uint64_t demand = polling.acquire_ticket();
  bool received = polling.timedwait(demand, 1000ULL);
  EXPECT_FALSE(received);
  while (true) {
    received = polling.timedwait(demand, 10000ULL);
    if (received) {
      break;
    }
  }

  t.join();
}

}  // namespace soc
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SharedPollingTest, foedus.soc);
