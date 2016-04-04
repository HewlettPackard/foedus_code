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

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/thread/rendezvous_impl.hpp"

/**
 * @file test_rendezvous.cpp
 * Testcases for foedus::thread::Rendezvous.
 */
namespace foedus {
namespace thread {
DEFINE_TEST_CASE_PACKAGE(RendezvousTest, foedus.thread);

TEST(RendezvousTest, Instantiate) {
  Rendezvous rendezvous;
}
TEST(RendezvousTest, Signal) {
  Rendezvous rendezvous;
  EXPECT_FALSE(rendezvous.is_signaled_weak());
  EXPECT_FALSE(rendezvous.is_signaled());
  rendezvous.signal();
  EXPECT_TRUE(rendezvous.is_signaled());
  EXPECT_TRUE(rendezvous.is_signaled_weak());
}



TEST(RendezvousTest, Simple) {
  Rendezvous rendezvous;
  std::atomic<bool> ends(false);
  // I don't like [=] it's a bit handwavy syntax.
  std::thread another([](Rendezvous *rendezvous_, std::atomic<bool> *ends_){
    rendezvous_->wait();
    ends_->store(true);
  }, &rendezvous, &ends);

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_FALSE(rendezvous.is_signaled_weak());
  EXPECT_FALSE(rendezvous.is_signaled());
  EXPECT_FALSE(ends.load());
  rendezvous.signal();
  EXPECT_TRUE(rendezvous.is_signaled());
  EXPECT_TRUE(rendezvous.is_signaled_weak());

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_TRUE(ends.load());
  another.join();
}

const int kRep = 100;  // 300;  otherwise this test case takes really long time depending on #cores.
const int kClients = 4;
std::vector<Rendezvous*>            many_rendezvous;
std::vector< std::atomic<int>* >    many_ends;
void many_thread() {
  for (int i = 0; i < kRep; ++i) {
    many_rendezvous[i]->wait();
    EXPECT_TRUE(many_rendezvous[i]->is_signaled());
    int added = ++(*many_ends[i]);
    if (added == kClients) {
      delete many_rendezvous[i];
    }
  }
}
TEST(RendezvousTest, Many) {
  if (RUNNING_ON_VALGRIND) {
    std::cout << "This test seems to take too long time on valgrind."
      << " There is nothing interesting in this test to run on valgrind, so"
      << " we simply skip this test." << std::endl;
    return;
  }

  // This tests 1) spurious wake up, 2) lost signal (spurious blocking), 3) and other anomalies.
  for (int i = 0; i < kRep; ++i) {
    many_rendezvous.push_back(new Rendezvous());
    many_ends.push_back(new std::atomic<int>(0));
  }

  std::vector<std::thread> clients;
  for (int i = 0; i < kClients; ++i) {
    clients.emplace_back(std::thread(&many_thread));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  for (int i = 0; i < kRep; ++i) {
    EXPECT_EQ(0, many_ends[i]->load());
  }

  for (int i = 0; i < kRep; ++i) {
    EXPECT_EQ(0, many_ends[i]->load());
    many_rendezvous[i]->signal();
    if (i % 3 == 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
  }

  for (int i = 0; i < kClients; ++i) {
    clients[i].join();
  }

  for (int i = 0; i < kRep; ++i) {
    EXPECT_EQ(kClients, many_ends[i]->load());
    delete many_ends[i];
  }
}

}  // namespace thread
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(RendezvousTest, foedus.thread);
