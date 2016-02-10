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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_mcs_adapter_impl.hpp"
#include "foedus/xct/xct_mcs_impl.hpp"

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(XctMcsImplTest, foedus.xct);

// Even IDs are readers, odd ones are writers
const int kThreads = 10;
const int kNodes = 1;  // this so far must be 1. otherwise thread-id is not contiguous. tedious.
const int kKeys = 100;

template<typename RW_BLOCK>
struct Runner {
  static void test_instantiate() {
    McsMockContext<RW_BLOCK> con;
    con.init(kNodes, kThreads / kNodes, 1U << 16);
    McsMockAdaptor<RW_BLOCK> adaptor(0, &con);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
  }

  McsMockContext<RW_BLOCK> context;
  McsRwLock keys[kKeys];
  std::atomic<bool> locked[kThreads];
  std::atomic<bool> done[kThreads];
  std::atomic<bool> signaled;
  std::atomic<int> locked_count;
  std::atomic<int> done_count;

  void sleep_enough() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  void init() {
    context.init(kNodes, kThreads / kNodes, 1U << 16);
    for (int i = 0; i < kKeys; ++i) {
      keys[i].reset();
      EXPECT_FALSE(keys[i].is_locked());
    }
    for (int i = 0; i < kThreads; ++i) {
      done[i] = false;
      locked[i] = false;
    }
    locked_count = 0;
    done_count = 0;
    signaled = false;
  }

  void no_conflict_task(thread::ThreadId id) {
    McsMockAdaptor<RW_BLOCK> adaptor(id, &context);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
    McsBlockIndex block = 0;
    if (id % 2 == 0) {
      block = impl.acquire_unconditional_rw_reader(&keys[id]);
    } else {
      block = impl.acquire_unconditional_rw_writer(&keys[id]);
    }
    locked[id] = true;
    ++locked_count;
    while (!signaled) {
      sleep_enough();
      assorted::memory_fence_seq_cst();
    }
    if (id % 2 == 0) {
      impl.release_rw_reader(&keys[id], block);
    } else {
      impl.release_rw_writer(&keys[id], block);
    }
    done[id] = true;
    ++done_count;
  }

  void test_no_conflict() {
    init();
    std::vector<std::thread> sessions;
    for (int i = 0; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::no_conflict_task, this, i);
    }

    while (locked_count < kThreads) {
      sleep_enough();
    }

    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(keys[i].is_locked());
      EXPECT_TRUE(locked[i]);
      EXPECT_FALSE(done[i]);
    }
    signaled = true;
    while (done_count < kThreads) {
      sleep_enough();
    }
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(locked[i]);
      EXPECT_TRUE(done[i]);
      EXPECT_FALSE(keys[i].is_locked());
    }
    for (int i = 0; i < kThreads; ++i) {
      sessions[i].join();
    }
  }

  void conflict_task(thread::ThreadId id) {
    McsMockAdaptor<RW_BLOCK> adaptor(id, &context);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
    int l = id < kThreads / 2 ? id : id - kThreads / 2;
    McsBlockIndex block = 0;
    if (id % 2 == 0) {
      block = impl.acquire_unconditional_rw_reader(&keys[l]);
      LOG(INFO) << "Acked-" << id << " on " << l << " Reader";
    } else {
      block = impl.acquire_unconditional_rw_writer(&keys[l]);
      LOG(INFO) << "Acked-" << id << " on " << l << " Writer";
    }
    locked[id] = true;
    ++locked_count;
    while (!signaled) {
      sleep_enough();
    }
    if (id % 2 == 0) {
      impl.release_rw_reader(&keys[l], block);
    } else {
      impl.release_rw_writer(&keys[l], block);
    }
    done[id] = true;
    ++done_count;
  }

  void test_conflict() {
    init();
    std::vector<std::thread> sessions;
    for (int i = 0; i < kThreads / 2; ++i) {
      sessions.emplace_back(&Runner::conflict_task, this, i);
    }
    LOG(INFO) << "Launched 1st half";
    while (locked_count < kThreads / 2) {
      sleep_enough();
    }
    for (int i = kThreads / 2; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::conflict_task, this, i);
    }
    LOG(INFO) << "Launched 2nd half";
    for (int i = 0; i < 4; ++i) {
      sleep_enough();
    }
    LOG(INFO) << "Should be done by now";
    for (int i = 0; i < kThreads; ++i) {
      int l = i < kThreads / 2 ? i : i - kThreads / 2;
      EXPECT_TRUE(keys[l].is_locked()) << i;
      if (i < kThreads / 2) {
        EXPECT_TRUE(locked[i]) << i;
      } else {
        EXPECT_FALSE(locked[i]) << i;
      }
      EXPECT_FALSE(done[i]) << i;
    }
    signaled = true;
    while (done_count < kThreads) {
      sleep_enough();
    }
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(locked[i]) << i;
      EXPECT_TRUE(done[i]) << i;
      EXPECT_FALSE(keys[i].is_locked()) << i;
    }
    for (int i = 0; i < kThreads; ++i) {
      sessions[i].join();
    }
  }

  void random_task(thread::ThreadId id) {
    McsMockAdaptor<RW_BLOCK> adaptor(id, &context);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
    assorted::UniformRandom r(id);
    for (uint32_t i = 0; i < 1000; ++i) {
      uint32_t k = r.uniform_within(0, kKeys - 1);
      McsBlockIndex block = 0;
      if (id % 2 == 0) {
        block = impl.acquire_unconditional_rw_reader(&keys[k]);
        impl.release_rw_reader(&keys[k], block);
      } else {
        block = impl.acquire_unconditional_rw_writer(&keys[k]);
        impl.release_rw_writer(&keys[k], block);
      }
    }
    ++done_count;
    done[id] = true;
  }

  void test_random() {
    init();
    std::vector<std::thread> sessions;
    for (int i = 0; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::random_task, this, i);
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    for (int i = 0; i < kKeys; ++i) {
      EXPECT_FALSE(keys[i].is_locked());
    }
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(done[i]) << i;
    }
    for (int i = 0; i < kThreads; ++i) {
      sessions[i].join();
    }
  }
};

TEST(XctMcsImplTest, InstantiateSimple) { Runner<McsRwSimpleBlock>::test_instantiate(); }
TEST(XctMcsImplTest, InstantiateExtended) { Runner<McsRwExtendedBlock>::test_instantiate(); }

TEST(XctMcsImplTest, NoConflictSimple) { Runner<McsRwSimpleBlock>().test_no_conflict(); }
TEST(XctMcsImplTest, NoConflictExtended) { Runner<McsRwExtendedBlock>().test_no_conflict(); }

TEST(XctMcsImplTest, ConflictSimple) { Runner<McsRwSimpleBlock>().test_conflict(); }
TEST(XctMcsImplTest, ConflictExtended) { Runner<McsRwExtendedBlock>().test_conflict(); }

TEST(XctMcsImplTest, RandomSimple) { Runner<McsRwSimpleBlock>().test_random(); }
TEST(XctMcsImplTest, RandomExtended) { Runner<McsRwExtendedBlock>().test_random(); }

}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(XctMcsImplTest, foedus.xct);
