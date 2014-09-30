/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
