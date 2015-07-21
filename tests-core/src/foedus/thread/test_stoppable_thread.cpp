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

TEST_MAIN_CAPTURE_SIGNALS(StoppableThreadTest, foedus.thread);
