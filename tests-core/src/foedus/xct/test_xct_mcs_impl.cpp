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
const int kMaxBlocks = 1U << 16;

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
    context.init(kNodes, kThreads / kNodes, kMaxBlocks);
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

  void spin() {
    assorted::UniformRandom spin_rnd(1234567);
    int32_t rounds = spin_rnd.uniform_within(1, 1000);
    while (--rounds) {}
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
    for (int i = kThreads / 2; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::conflict_task, this, i);
    }
    LOG(INFO) << "Launched 2nd half";

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

  /**
   * This one is a bit different from others.
   * Here we intentially let it fail by taking locks
   * in non-canonical mode. We just confirm that
   * "try" won't cause a deadlock then.
   *
   * Lock-0 and Lock-9 is raced by Thread-0 and Thread-9.
   * Thread-0 \e unconditionally takes Lock-0 as writer, then Lock-9 \e unconditionnally as writer.
   * Thread-9 \e unconditionally takes Lock-9 as writer, then Lock-0 with \e try.
   * Thread-9 might fail to take a lock, but it shouldn't cause an infinite wait.
   *
   * We do this for Lock-(n)/Lock-(9-n) by Thread-(n)/Thread-(9-n) and repeat it 100 times each.
   *
   * This testcase is to reproduce a bug we had and to verify the fix.
   */
  void non_canonical_task(thread::ThreadId id, bool latter_reader) {
    McsMockAdaptor<RW_BLOCK> adaptor(id, &context);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
    const bool i_am_latter = id >= kThreads / 2U;
    const int l1 = i_am_latter ? kThreads - 1 - id : id;
    const int l2 = i_am_latter ? id : kThreads - 1 - id;
#ifndef NDEBUG
    constexpr uint32_t kTries = 1000;
#else  // NDEBUG
    constexpr uint32_t kTries = 10000;
#endif  // NDEBUG
    if (kTries * 2U >= kMaxBlocks) {
      LOG(FATAL) << "wait, too large kTries. Increase kMaxBlocks to test it";
    }

    while (!signaled) {
      sleep_enough();
    }
    bool try_succeeded_at_least_once = false;
    for (uint32_t i = 0; i < kTries; ++i) {
      if (i_am_latter) {
        McsBlockIndex block1 = impl.acquire_unconditional_rw_writer(&keys[l1]);
        McsBlockIndex block2;
        if (latter_reader) {
          block2 = impl.acquire_try_rw_reader(&keys[l2]);
        } else {
          block2 = impl.acquire_try_rw_writer(&keys[l2]);
        }
        impl.release_rw_writer(&keys[l1], block1);
        if (block2) {
          try_succeeded_at_least_once = true;
          if (latter_reader) {
            impl.release_rw_reader(&keys[l2], block2);
          } else {
            impl.release_rw_writer(&keys[l2], block2);
          }
        }
      } else {
        McsBlockIndex block1 = impl.acquire_unconditional_rw_writer(&keys[l1]);
        McsBlockIndex block2 = impl.acquire_unconditional_rw_writer(&keys[l2]);
        impl.release_rw_writer(&keys[l1], block1);
        impl.release_rw_writer(&keys[l2], block2);
      }
    }
    if (i_am_latter && !try_succeeded_at_least_once) {
      // Maybe we make it an error? but this is possibble..
      LOG(WARNING) << "Really, no success at all? mm, it's possible, but suspecious";
      EXPECT_TRUE(try_succeeded_at_least_once);  // now really an error.
    }
    done[id] = true;
    ++done_count;
  }

  void test_non_canonical(bool latter_reader) {
    init();
    std::vector<std::thread> sessions;
    for (int i = 0; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::non_canonical_task, this, i, latter_reader);
    }
    signaled = true;
    while (done_count < kThreads) {
      sleep_enough();
    }
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(done[i]) << i;
      EXPECT_FALSE(keys[i].is_locked()) << i;
    }
    for (int i = 0; i < kThreads; ++i) {
      sessions[i].join();
    }
  }
  void test_non_canonical1() { test_non_canonical(false); }
  void test_non_canonical2() { test_non_canonical(true); }

  void async_read_only_task(thread::ThreadId id) {
    McsMockAdaptor<RW_BLOCK> adaptor(id, &context);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
    assorted::UniformRandom r(id);
    auto* lock = keys;
    for (int i = 0; i < 1000; ++i) {
      uint32_t timeout = r.uniform_within(0, 10000);
      if (timeout % 1000 == 0) {
        auto block_index = impl.acquire_unconditional_rw_reader(lock);
        EXPECT_GT(block_index, 0);
        spin();
        impl.release_rw_reader(lock, block_index);
      } else {
        auto ret = impl.acquire_async_rw_reader(lock);
        EXPECT_GT(ret.block_index_, 0);
        if (!ret.acquired_) {
          while (timeout--) {
            if (impl.retry_async_rw_reader(lock, ret.block_index_)) {
              ret.acquired_ = true;
              break;
            }
          }
        }
        if (ret.acquired_) {
          spin();
          impl.release_rw_reader(lock, ret.block_index_);
        } else {
          impl.cancel_async_rw_reader(lock, ret.block_index_);
        }
      }
    }
    ++done_count;
    done[id] = true;
  }

  void async_write_only_task(thread::ThreadId id) {
    McsMockAdaptor<RW_BLOCK> adaptor(id, &context);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
    assorted::UniformRandom r(id);
    auto* lock = keys;
    for (int i = 0; i < 1000; ++i) {
      uint32_t timeout = r.uniform_within(0, 10000);
      if (timeout % 1000 == 0) {
        auto block_index = impl.acquire_unconditional_rw_writer(lock);
        EXPECT_GT(block_index, 0);
        spin();
        impl.release_rw_writer(lock, block_index);
      } else {
        auto ret = impl.acquire_async_rw_writer(lock);
        EXPECT_GT(ret.block_index_, 0);
        if (!ret.acquired_) {
          while (timeout--) {
            if (impl.retry_async_rw_writer(lock, ret.block_index_)) {
              ret.acquired_ = true;
              break;
            }
          }
        }
        if (ret.acquired_) {
          spin();
          impl.release_rw_writer(lock, ret.block_index_);
        } else {
          impl.cancel_async_rw_writer(lock, ret.block_index_);
        }
      }
    }
    ++done_count;
    done[id] = true;
  }

  void async_read_write_task(thread::ThreadId id) {
    McsMockAdaptor<RW_BLOCK> adaptor(id, &context);
    McsImpl< McsMockAdaptor<RW_BLOCK> , RW_BLOCK> impl(adaptor);
    assorted::UniformRandom r(id);
    auto* lock = keys;
    for (int i = 0; i < 1000; ++i) {
      uint32_t timeout = r.uniform_within(0, 10000);
      if (i % 2 == 0) {
        if (timeout % 1000 == 0) {
          auto block_index = impl.acquire_unconditional_rw_reader(lock);
          EXPECT_GT(block_index, 0);
          spin();
          impl.release_rw_reader(lock, block_index);
        } else {
          auto ret = impl.acquire_async_rw_reader(lock);
          EXPECT_GT(ret.block_index_, 0);
          if (!ret.acquired_) {
            while (timeout--) {
              if (impl.retry_async_rw_reader(lock, ret.block_index_)) {
                ret.acquired_ = true;
                break;
              }
            }
          }
          if (ret.acquired_) {
            spin();
            impl.release_rw_reader(lock, ret.block_index_);
          } else {
            impl.cancel_async_rw_reader(lock, ret.block_index_);
          }
        }
      } else {
        if (timeout % 1000 == 0) {
          auto block_index = impl.acquire_unconditional_rw_writer(lock);
          EXPECT_GT(block_index, 0);
          spin();
          impl.release_rw_writer(lock, block_index);
        } else {
          auto ret = impl.acquire_async_rw_writer(lock);
          EXPECT_GT(ret.block_index_, 0);
          if (!ret.acquired_) {
            while (timeout--) {
              if (impl.retry_async_rw_writer(lock, ret.block_index_)) {
                ret.acquired_ = true;
                break;
              }
            }
          }
          if (ret.acquired_) {
            spin();
            impl.release_rw_writer(lock, ret.block_index_);
          } else {
            impl.cancel_async_rw_writer(lock, ret.block_index_);
          }
        }
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

  void test_async_read_only() {
    init();
    std::vector<std::thread> sessions;
    for (int i = 0; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::async_read_only_task, this, i);
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    EXPECT_FALSE(keys[0].is_locked());
    EXPECT_TRUE(done[0]);
    for (int i = 0; i < kThreads; ++i) {
      sessions[i].join();
    }
  }

  void test_async_write_only() {
    init();
    std::vector<std::thread> sessions;
    for (int i = 0; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::async_write_only_task, this, i);
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    EXPECT_FALSE(keys[0].is_locked());
    EXPECT_TRUE(done[0]);
    for (int i = 0; i < kThreads; ++i) {
      sessions[i].join();
    }
  }

  void test_async_read_write() {
    init();
    std::vector<std::thread> sessions;
    for (int i = 0; i < kThreads; ++i) {
      sessions.emplace_back(&Runner::async_read_write_task, this, i);
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    EXPECT_FALSE(keys[0].is_locked());
    EXPECT_TRUE(done[0]);
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

TEST(XctMcsImplTest, NonCanonical1Simple) { Runner<McsRwSimpleBlock>().test_non_canonical1(); }
TEST(XctMcsImplTest, NonCanonical1Extended) { Runner<McsRwExtendedBlock>().test_non_canonical1(); }
TEST(XctMcsImplTest, NonCanonical2Simple) { Runner<McsRwSimpleBlock>().test_non_canonical2(); }
TEST(XctMcsImplTest, NonCanonical2Extended) { Runner<McsRwExtendedBlock>().test_non_canonical2(); }

TEST(XctMcsImplTest, RandomSimple) { Runner<McsRwSimpleBlock>().test_random(); }
TEST(XctMcsImplTest, RandomExtended) { Runner<McsRwExtendedBlock>().test_random(); }

TEST(XctMcsImplTest, AsyncReadOnlySimple) { Runner<McsRwSimpleBlock>().test_async_read_only(); }
TEST(XctMcsImplTest, AsyncReadOnlyExtended) { Runner<McsRwExtendedBlock>().test_async_read_only(); }

TEST(XctMcsImplTest, AsyncWriteOnlySimple) { Runner<McsRwSimpleBlock>().test_async_write_only(); }
TEST(XctMcsImplTest, AsyncWriteOnlyExtended) {
  Runner<McsRwExtendedBlock>().test_async_write_only();
}

TEST(XctMcsImplTest, AsyncReadWriteSimple) { Runner<McsRwSimpleBlock>().test_async_read_write(); }
TEST(XctMcsImplTest, AsyncReadWriteExtended) {
  Runner<McsRwExtendedBlock>().test_async_read_write();
}
}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(XctMcsImplTest, foedus.xct);
