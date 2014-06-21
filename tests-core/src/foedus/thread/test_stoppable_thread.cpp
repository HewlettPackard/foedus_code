/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>
#include <stdint.h>

#include <chrono>
#include <thread>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"

/**
 * @file test_stoppable_thread.cpp
 * Testcases for foedus::thread::StoppableThread.
 */

namespace foedus {
namespace thread {
DEFINE_TEST_CASE_PACKAGE(StoppableThreadTest, foedus.thread);

void handle_thread(StoppableThread* me) {
  while (!me->sleep()) {
  }
}

TEST(StoppableThreadTest, Minimal) {
  for (int sleep_ms = 0; sleep_ms < 50; sleep_ms += 10) {
    StoppableThread th;
    th.initialize("test", std::move(std::thread(handle_thread, &th)),
            std::chrono::milliseconds(5));
    EXPECT_FALSE(th.is_stop_requested());
    EXPECT_FALSE(th.is_stopped());
    EXPECT_FALSE(th.is_stop_requested_weak());
    EXPECT_FALSE(th.is_stopped_weak());
    if (sleep_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
    th.stop();
    EXPECT_TRUE(th.is_stop_requested());
    EXPECT_TRUE(th.is_stopped());
    EXPECT_TRUE(th.is_stop_requested_weak());
    EXPECT_TRUE(th.is_stopped_weak());
  }
}

TEST(StoppableThreadTest, Wakeup) {
  StoppableThread th;
  th.initialize("test", std::move(std::thread(handle_thread, &th)), std::chrono::milliseconds(5));
  EXPECT_FALSE(th.is_stop_requested());
  EXPECT_FALSE(th.is_stopped());
  EXPECT_FALSE(th.is_stop_requested_weak());
  EXPECT_FALSE(th.is_stopped_weak());
  th.wakeup();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  th.wakeup();
  th.stop();
  EXPECT_TRUE(th.is_stop_requested());
  EXPECT_TRUE(th.is_stopped());
  EXPECT_TRUE(th.is_stop_requested_weak());
  EXPECT_TRUE(th.is_stopped_weak());
}

TEST(StoppableThreadTest, Many) {
  std::vector<StoppableThread*> threads;
  const int kThreads = 10;
  for (int i = 0; i < kThreads; ++i) {
    StoppableThread* th = new StoppableThread();
    threads.push_back(th);
    th->initialize("test", i,
             std::move(std::thread(handle_thread, th)), std::chrono::milliseconds(5));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  for (int i = 0; i < kThreads; ++i) {
    StoppableThread* th = threads[i];
    EXPECT_FALSE(th->is_stop_requested());
    EXPECT_FALSE(th->is_stopped());
    EXPECT_FALSE(th->is_stop_requested_weak());
    EXPECT_FALSE(th->is_stopped_weak());
    th->stop();
    EXPECT_TRUE(th->is_stop_requested());
    EXPECT_TRUE(th->is_stopped());
    EXPECT_TRUE(th->is_stop_requested_weak());
    EXPECT_TRUE(th->is_stopped_weak());
    delete th;
  }
}

}  // namespace thread
}  // namespace foedus
