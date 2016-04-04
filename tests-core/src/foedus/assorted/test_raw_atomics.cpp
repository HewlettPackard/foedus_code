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
#include <valgrind.h>
#include <gtest/gtest.h>

#include <iostream>
#include <thread>
#include <vector>

#include "foedus/test_common.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/thread/rendezvous_impl.hpp"

namespace foedus {
namespace assorted {

const int kThreads = 6;
const int kIterations = 20;
const int kReps = 5;
template <typename T>
struct CasTestImpl {
  explicit CasTestImpl(bool weak) : weak_(weak) {}
  void test() {
    static_assert(kThreads * kIterations < 127, "exceeds smallest integer");
    data_ = 0;
    conflicts_ = 0;
    for (int i = 0; i <= kThreads * kIterations; ++i) {
      observed_[i] = false;
    }
    assorted::memory_fence_release();
    for (int i = 0; i < kThreads; ++i) {
      threads_.emplace_back(std::thread(&CasTestImpl::handle, this, i));
    }

    start_rendezvous_.signal();
    std::cout << "Started execution" << std::endl;
    for (int i = 0; i < kThreads; ++i) {
      threads_[i].join();
    }
    threads_.clear();

    EXPECT_FALSE(observed_[data_]) << data_;
    observed_[data_] = true;
    assorted::memory_fence_acquire();
    for (int i = 0; i <= kThreads * kIterations; ++i) {
      EXPECT_TRUE(observed_[i]) << i;
    }
    std::cout << "In total, about " << conflicts_ << " coflicts" << std::endl;
  }

  void handle(int id) {
    start_rendezvous_.wait();
    EXPECT_GE(id, 0);
    EXPECT_LT(id, kThreads);
    for (int i = 0; i < kIterations; ++i) {
      T old = 127;
      const T desired = value(id, i);
      bool swapped;
      if (weak_) {
        swapped = assorted::raw_atomic_compare_exchange_weak<T>(&data_, &old, desired);
      } else {
        swapped = assorted::raw_atomic_compare_exchange_strong<T>(&data_, &old, desired);
      }
      EXPECT_FALSE(swapped);
      EXPECT_NE(desired, old);
      EXPECT_NE(127, old);
      SPINLOCK_WHILE(true) {
        T prev_old = old;
        if (weak_) {
          swapped = assorted::raw_atomic_compare_exchange_weak<T>(&data_, &old, desired);
        } else {
          swapped = assorted::raw_atomic_compare_exchange_strong<T>(
            &data_, &old, desired);
        }
        if (swapped) {
          EXPECT_EQ(prev_old, old);
          EXPECT_FALSE(observed_[old]);
          observed_[old] = true;
          break;
        } else {
          ++conflicts_;
          EXPECT_NE(prev_old, old);
          // to speed-up valgrind tests.
          std::this_thread::sleep_for(std::chrono::seconds(0));
          assorted::memory_fence_acq_rel();
        }
      }
    }
  }
  T value(int id, int iteration) const {
    return iteration * kThreads + id + 1;
  }

  bool weak_;
  std::vector<std::thread> threads_;
  thread::Rendezvous start_rendezvous_;
  T data_;
  int conflicts_;
  bool observed_[(kThreads * kIterations) / 8 * 8 + 8];
};
template <typename T>
struct CasTest {
  explicit CasTest(bool weak = false) : weak_(weak) {}
  void test() {
    if (RUNNING_ON_VALGRIND) {
      std::cout << "This test seems to take too long time on valgrind."
        << " There is nothing interesting in this test to run on valgrind, so"
        << " we simply skip this test." << std::endl;
      return;
    }
    for (int i = 0; i < kReps; ++i) {
      CasTestImpl<T> impl(weak_);
      impl.test();
    }
  }
  bool weak_;
};

DEFINE_TEST_CASE_PACKAGE(RawAtomicsTest, foedus.assorted);

TEST(RawAtomicsTest, Uint8) { CasTest<uint8_t>().test(); }
TEST(RawAtomicsTest, Uint16) { CasTest<uint16_t>().test(); }
TEST(RawAtomicsTest, Uint32) { CasTest<uint32_t>().test(); }
TEST(RawAtomicsTest, Uint64) { CasTest<uint64_t>().test(); }
TEST(RawAtomicsTest, Int8) { CasTest<int8_t>().test(); }
TEST(RawAtomicsTest, Int16) { CasTest<int16_t>().test(); }
TEST(RawAtomicsTest, Int32) { CasTest<int32_t>().test(); }
TEST(RawAtomicsTest, Int64) { CasTest<int64_t>().test(); }

TEST(RawAtomicsTest, Uint8Weak) { CasTest<uint8_t>(true).test(); }
TEST(RawAtomicsTest, Uint16Weak) { CasTest<uint16_t>(true).test(); }
TEST(RawAtomicsTest, Uint32Weak) { CasTest<uint32_t>(true).test(); }
TEST(RawAtomicsTest, Uint64Weak) { CasTest<uint64_t>(true).test(); }
TEST(RawAtomicsTest, Int8Weak) { CasTest<int8_t>(true).test(); }
TEST(RawAtomicsTest, Int16Weak) { CasTest<int16_t>(true).test(); }
TEST(RawAtomicsTest, Int32Weak) { CasTest<int32_t>(true).test(); }
TEST(RawAtomicsTest, Int64Weak) { CasTest<int64_t>(true).test(); }

}  // namespace assorted
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(RawAtomicsTest, foedus.assorted);
