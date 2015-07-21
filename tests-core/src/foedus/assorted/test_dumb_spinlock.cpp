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
#include <stdint.h>
#include <gtest/gtest.h>

#include <iostream>
#include <thread>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/dumb_spinlock.hpp"
#include "foedus/thread/rendezvous_impl.hpp"

namespace foedus {
namespace assorted {

const int kIterations = 3000;
bool locked;
int total;
void test_internal(thread::Rendezvous* start_rendezvous) {
  start_rendezvous->wait();
  for (int i = 0; i < kIterations; ++i) {
    DumbSpinlock the_lock(&locked);
    EXPECT_TRUE(the_lock.is_locked_by_me());
    ++total;
  }
}
void test_internal_lazy(thread::Rendezvous* start_rendezvous) {
  start_rendezvous->wait();
  for (int i = 0; i < kIterations; ++i) {
    DumbSpinlock the_lock(&locked, false);
    EXPECT_FALSE(the_lock.is_locked_by_me());
    the_lock.lock();
    EXPECT_TRUE(the_lock.is_locked_by_me());
    ++total;
    the_lock.unlock();
  }
}

void test(uint32_t thread_count, bool lazy = false) {
  locked = false;
  total = 0;
  thread::Rendezvous start_rendezvous;
  assorted::memory_fence_release();
  std::vector< std::thread > threads;
  for (uint32_t i = 0; i < thread_count; ++i) {
    threads.emplace_back(std::thread(lazy ? test_internal_lazy : test_internal, &start_rendezvous));
  }

  start_rendezvous.signal();
  for (auto& t : threads) {
    t.join();
  }

  EXPECT_FALSE(locked);
  EXPECT_EQ(kIterations * thread_count, total);
}
DEFINE_TEST_CASE_PACKAGE(DumbSpinlockTest, foedus.assorted);

TEST(DumbSpinlockTest, OneThread) { test(1); }
TEST(DumbSpinlockTest, TwoThreads) { test(2); }
TEST(DumbSpinlockTest, FourThreads) { test(4); }
TEST(DumbSpinlockTest, SixThreads) { test(6); }

TEST(DumbSpinlockTest, OneThreadLazy) { test(1, true); }
TEST(DumbSpinlockTest, TwoThreadsLazy) { test(2, true); }
TEST(DumbSpinlockTest, FourThreadsLazy) { test(4, true); }
TEST(DumbSpinlockTest, SixThreadsLazy) { test(6, true); }

}  // namespace assorted
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(DumbSpinlockTest, foedus.assorted);
