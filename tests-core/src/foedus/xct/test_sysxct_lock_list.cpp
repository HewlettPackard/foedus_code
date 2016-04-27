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
#include <chrono>
#include <thread>

#include "foedus/test_common.hpp"
#include "foedus/assorted/spin_until_impl.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/xct/sysxct_impl.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_mcs_adapter_impl.hpp"
#include "foedus/xct/xct_mcs_impl.hpp"

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(SysxctLockListTest, foedus.xct);

const uint16_t kNodes = 1U;
const uint16_t kDummyStorageId = 1U;
const uint16_t kDefaultNodeId = 0U;

/**
 * A test to take record locks in sorted/reversed order.
 */
template<typename RW_BLOCK>
void test_order_record_impl(bool reversed) {
  const uint32_t kMaxLockCount = 1 << 10;  // big so that they are in different pages
  RwLockableXctId* lock_addresses[kMaxLockCount];
  UniversalLockId lock_ids[kMaxLockCount];
  storage::VolatilePagePointer page_ids[kMaxLockCount];
  McsMockContext< RW_BLOCK > con;
  con.init(kDummyStorageId, kNodes, 1U, 1U << 16, kMaxLockCount);
  for (uint32_t i = 0; i < kMaxLockCount; ++i) {
    lock_addresses[i] = con.get_rw_lock_address(kDefaultNodeId, i);
    lock_addresses[i]->xct_id_.set_epoch_int(i + 1U);
    lock_ids[i] = to_universal_lock_id(
      con.page_memory_resolver_,
      reinterpret_cast<uintptr_t>(lock_addresses[i]));
    page_ids[i] = storage::to_page(lock_addresses[i])->get_volatile_page_id();
  }

  const uint32_t kOddCount = (kMaxLockCount + 1U) / 2U;
  uint32_t odd_indexes[kOddCount];
  uint32_t odd_indexes_after_sort[kOddCount];
  for (uint32_t oi = 0; oi < kOddCount; ++oi) {
    if (reversed) {
      odd_indexes[oi] = (kOddCount - oi) * 2U - 1U;
      odd_indexes_after_sort[oi] = oi * 2U + 1U;
    } else {
      odd_indexes[oi] = oi * 2U + 1U;
      odd_indexes_after_sort[oi] = odd_indexes[oi];
    }
  }

  McsMockAdaptor< RW_BLOCK > adaptor(0, &con);

  SysxctLockList list;
  list.init();
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  EXPECT_EQ(list.cend(), list.cbegin());
  EXPECT_EQ(list.end(), list.begin());

  // Odd numbered records
  for (uint32_t oi = 0; oi < kOddCount; ++oi) {
    uint32_t i = odd_indexes[oi];
    ErrorCode ret = list.request_record_lock(adaptor, page_ids[i], lock_addresses[i]);
    EXPECT_EQ(kErrorCodeOk, ret);
    EXPECT_EQ(oi + 1U, list.get_last_active_entry());
    EXPECT_EQ(oi + 1U, list.get_last_locked_entry());
  }

  EXPECT_EQ(kOddCount, list.get_last_active_entry());
  EXPECT_EQ(kOddCount, list.get_last_locked_entry());

  // Check the entries after sorting
  for (uint32_t oi = 0; oi < kOddCount; ++oi) {
    uint32_t i = odd_indexes_after_sort[oi];
    auto* entry = list.get_entry(oi + 1U);
    EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
    EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
    EXPECT_TRUE(entry->is_locked());
    EXPECT_FALSE(entry->page_lock_);
    EXPECT_TRUE(entry->used_in_this_run_);
  }

  // Do the same with iterator
  for (auto entry = list.cbegin(); entry != list.cend(); ++entry) {
    uint32_t i = list.to_pos(entry) * 2U - 1U;
    EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
    EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
    EXPECT_TRUE(entry->is_locked());
    EXPECT_FALSE(entry->page_lock_);
    EXPECT_TRUE(entry->used_in_this_run_);
  }

  LOG(INFO) << list;

  list.release_all_locks(adaptor);

  // Check the entries after release
  EXPECT_EQ(kOddCount, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  for (uint32_t oi = 0; oi < kOddCount; ++oi) {
    uint32_t i = odd_indexes_after_sort[oi];
    auto* entry = list.get_entry(oi + 1U);
    EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
    EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
    EXPECT_FALSE(entry->is_locked());
    EXPECT_FALSE(entry->page_lock_);
    EXPECT_TRUE(entry->used_in_this_run_);
  }

  // Do the same with iterator
  for (auto entry = list.cbegin(); entry != list.cend(); ++entry) {
    uint32_t i = list.to_pos(entry) * 2U - 1U;
    EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
    EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
    EXPECT_FALSE(entry->is_locked());
    EXPECT_FALSE(entry->page_lock_);
    EXPECT_TRUE(entry->used_in_this_run_);
  }

  LOG(INFO) << list;

  list.clear_entries(kLockListPositionInvalid);

  EXPECT_EQ(kLockListPositionInvalid, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  EXPECT_EQ(list.cend(), list.cbegin());
  EXPECT_EQ(list.end(), list.begin());
}
void test_order_record(bool extended_lock, bool reversed) {
  if (extended_lock) {
    test_order_record_impl< McsRwExtendedBlock >(reversed);
  } else {
    test_order_record_impl< McsRwSimpleBlock >(reversed);
  }
}

/**
 * Specifically tests the is_try_mode_required() function.
 * Directly uses batch_get_or_add_entries().
 */
struct TestIsTryRequired {
  void run_record() {
    const uint32_t kMaxLockCount = 1 << 10;  // big so that they are in different pages
    const uint32_t kAcquirePer = 10U;
    // eg 1->1, 10->1, 11->2, 20->2, ...
    const uint32_t kTotalAcquires = (kMaxLockCount - 1U) / kAcquirePer + 1U;
    RwLockableXctId* lock_addresses[kMaxLockCount];
    UniversalLockId lock_ids[kMaxLockCount];
    storage::VolatilePagePointer page_ids[kMaxLockCount];
    McsMockContext< McsRwSimpleBlock > con;
    con.init(kDummyStorageId, kNodes, 1U, 1U << 16, kMaxLockCount);
    for (uint32_t i = 0; i < kMaxLockCount; ++i) {
      lock_addresses[i] = con.get_rw_lock_address(kDefaultNodeId, i);
      lock_addresses[i]->xct_id_.set_epoch_int(i + 1U);
      lock_ids[i] = to_universal_lock_id(
        con.page_memory_resolver_,
        reinterpret_cast<uintptr_t>(lock_addresses[i]));
      page_ids[i] = storage::to_page(lock_addresses[i])->get_volatile_page_id();
    }

    McsMockAdaptor< McsRwSimpleBlock > adaptor(0, &con);

    SysxctLockList list;
    list.init();
    EXPECT_EQ(kLockListPositionInvalid, list.get_last_active_entry());
    EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
    EXPECT_EQ(list.cend(), list.cbegin());
    EXPECT_EQ(list.end(), list.begin());

    uintptr_t run_addresses[kMcsMockDataPageLocksPerPage];
    uint32_t run_count = 1;
    uint32_t added_count = 0;
    storage::VolatilePagePointer run_page_id = page_ids[0];
    run_addresses[0] = reinterpret_cast<uintptr_t>(lock_addresses[0]);
    for (uint32_t i = kAcquirePer; i < kMaxLockCount; i += kAcquirePer) {
      // Batch up records in the same page
      if (run_page_id == page_ids[i]) {
        run_addresses[run_count] = reinterpret_cast<uintptr_t>(lock_addresses[i]);
        ++run_count;
        continue;
      }
      ASSERT_ND(run_count < kMcsMockDataPageLocksPerPage);
      ASSERT_ND(run_count);
      // In soviet C++, friends can see your private
      auto pos = list.batch_get_or_add_entries(
        run_page_id,
        run_count,
        run_addresses,
        false);
      EXPECT_EQ(added_count + 1U, pos);
      EXPECT_EQ(pos + run_count - 1U, list.get_last_active_entry());
      EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
      std::cout << "Batch of " << run_count << std::endl;

      added_count += run_count;
      run_count = 1;
      run_page_id = page_ids[i];
      run_addresses[0] = reinterpret_cast<uintptr_t>(lock_addresses[i]);
    }
    if (run_count) {
      auto pos = list.batch_get_or_add_entries(
        run_page_id,
        run_count,
        run_addresses,
        false);
      std::cout << "Last batch of " << run_count << std::endl;
      EXPECT_EQ(added_count + 1U, pos);
      added_count += run_count;
    }

    EXPECT_EQ(kTotalAcquires, added_count);
    EXPECT_EQ(kTotalAcquires, list.get_last_active_entry());
    EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());

    // In all cases, they are now sorted
    for (uint32_t pos = 1U; pos <= kTotalAcquires; ++pos) {
      auto* entry = list.get_entry(pos);
      uint32_t i = (pos - 1U) * kAcquirePer;
      EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
      EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
      EXPECT_FALSE(entry->is_locked());
      EXPECT_FALSE(entry->page_lock_);
      EXPECT_TRUE(entry->used_in_this_run_);
      EXPECT_FALSE(list.is_try_mode_required(pos));
    }

    // Do the same with iterator
    for (auto entry = list.cbegin(); entry != list.cend(); ++entry) {
      LockListPosition pos = list.to_pos(entry);
      uint32_t i = (pos - 1U) * kAcquirePer;
      EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
      EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
      EXPECT_FALSE(entry->is_locked());
      EXPECT_FALSE(entry->page_lock_);
      EXPECT_TRUE(entry->used_in_this_run_);
      EXPECT_FALSE(list.is_try_mode_required(pos));
    }

    LOG(INFO) << list;

    // What about with enclosing_max_lock_id_?
    LockListPosition boundary = kTotalAcquires / 2U;
    list.enclosing_max_lock_id_ = list.get_entry(boundary)->universal_lock_id_;
    for (uint32_t pos = 1U; pos <= kTotalAcquires; ++pos) {
      if (pos <= boundary) {
        EXPECT_TRUE(list.is_try_mode_required(pos));
      } else {
        EXPECT_FALSE(list.is_try_mode_required(pos));
      }
    }

    // Then lock the boundary ourselves and try again.
    uint32_t boundary_i = (boundary - 1U) * kAcquirePer;
    EXPECT_EQ(lock_ids[boundary_i], list.get_entry(boundary)->universal_lock_id_);
    ErrorCode ret
      = list.request_record_lock(adaptor, page_ids[boundary_i], lock_addresses[boundary_i]);
    EXPECT_EQ(kErrorCodeOk, ret);
    EXPECT_EQ(boundary, list.get_last_locked_entry());
    list.enclosing_max_lock_id_ = kNullUniversalLockId;
    for (uint32_t pos = 1U; pos <= kTotalAcquires; ++pos) {
      if (pos <= boundary) {
        EXPECT_TRUE(list.is_try_mode_required(pos));
      } else {
        EXPECT_FALSE(list.is_try_mode_required(pos));
      }
    }
  }
};

/**
 * Tests records and pages mixed.
 */
template<typename RW_BLOCK>
void test_order_mixed_impl(bool reversed) {
  const uint32_t kMaxLockCount = kMcsMockDataPageLocksPerPage * 6U;
  RwLockableXctId* lock_addresses[kMaxLockCount];
  UniversalLockId lock_ids[kMaxLockCount];
  storage::VolatilePagePointer page_ids[kMaxLockCount];
  McsMockContext< RW_BLOCK > con;
  con.init(kDummyStorageId, kNodes, 1U, 1U << 16, kMaxLockCount);
  for (uint32_t i = 0; i < kMaxLockCount; ++i) {
    lock_addresses[i] = con.get_rw_lock_address(kDefaultNodeId, i);
    lock_addresses[i]->xct_id_.set_epoch_int(i + 1U);
    page_ids[i] = storage::to_page(lock_addresses[i])->get_volatile_page_id();
    lock_ids[i] = to_universal_lock_id(
      page_ids[i].get_numa_node(),
      page_ids[i].get_offset(),
      reinterpret_cast<uintptr_t>(lock_addresses[i]));
  }

  McsMockAdaptor< RW_BLOCK > adaptor(0, &con);

  SysxctLockList list;
  list.init();
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  EXPECT_EQ(list.cend(), list.cbegin());
  EXPECT_EQ(list.end(), list.begin());

  auto* page2 = storage::to_page(lock_addresses[kMcsMockDataPageLocksPerPage]);
  auto* page3 = storage::to_page(lock_addresses[kMcsMockDataPageLocksPerPage * 2U]);
  auto* page4 = storage::to_page(lock_addresses[kMcsMockDataPageLocksPerPage * 3U]);

  // Take a mix of page and record locks in 2nd and 4th page. 3rd page only page lock.
  for (uint32_t i = 0; i < 3U; ++i) {
    if (i != 1U) {
      // mix of page and record locks
      uint32_t begin_i;
      storage::Page* page;
      if ((reversed && i == 0) || (!reversed && i == 2U)) {
        page = page2;
        begin_i = kMcsMockDataPageLocksPerPage;  // 2nd page
      } else {
        page = page4;
        begin_i = kMcsMockDataPageLocksPerPage * 3U;  // 4th page
      }
      storage::VolatilePagePointer page_id = page->get_volatile_page_id();
      EXPECT_EQ(kErrorCodeOk, list.request_page_lock(adaptor, page));
      ErrorCode ret = list.batch_request_record_locks(
        adaptor,
        page_id,
        kMcsMockDataPageLocksPerPage,
        lock_addresses + begin_i);
      EXPECT_EQ(kErrorCodeOk, ret);
    } else {
      // just page lock
      EXPECT_EQ(kErrorCodeOk, list.request_page_lock(adaptor, page3));
    }
  }

  uint32_t total_count = kMcsMockDataPageLocksPerPage * 2U + 3U;
  EXPECT_EQ(total_count, list.get_last_active_entry());
  EXPECT_EQ(total_count, list.get_last_locked_entry());

  // Check the entries after sorting
  for (LockListPosition pos = 1U; pos <= list.get_last_active_entry(); ++pos) {
    auto* entry = list.get_entry(pos);
    EXPECT_TRUE(entry->used_in_this_run_);
    EXPECT_TRUE(entry->is_locked());
    if (pos == 1U) {
      EXPECT_EQ(page2, entry->get_as_page_lock());
      EXPECT_TRUE(entry->page_lock_);
    } else if (pos <= kMcsMockDataPageLocksPerPage + 1U) {
      uint32_t pos_in_page = pos - 2U;
      ASSERT_ND(pos_in_page < kMcsMockDataPageLocksPerPage);
      uint32_t i = pos_in_page + kMcsMockDataPageLocksPerPage;
      EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
      EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
      EXPECT_FALSE(entry->page_lock_);
    } else if (pos == kMcsMockDataPageLocksPerPage + 2U) {
      EXPECT_EQ(page3, entry->get_as_page_lock());
      EXPECT_TRUE(entry->page_lock_);
    } else if (pos == kMcsMockDataPageLocksPerPage + 3U) {
      EXPECT_EQ(page4, entry->get_as_page_lock());
      EXPECT_TRUE(entry->page_lock_);
    } else {
      uint32_t pos_in_page = pos - kMcsMockDataPageLocksPerPage - 4U;
      ASSERT_ND(pos_in_page < kMcsMockDataPageLocksPerPage);
      uint32_t i = pos_in_page + kMcsMockDataPageLocksPerPage * 3U;
      EXPECT_EQ(lock_addresses[i], entry->get_as_record_lock());
      EXPECT_EQ(lock_ids[i], entry->universal_lock_id_);
      EXPECT_FALSE(entry->page_lock_);
    }
  }

  LOG(INFO) << list;

  list.release_all_locks(adaptor);

  EXPECT_EQ(total_count, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());

  LOG(INFO) << list;

  list.clear_entries(kLockListPositionInvalid);

  EXPECT_EQ(kLockListPositionInvalid, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  EXPECT_EQ(list.cend(), list.cbegin());
  EXPECT_EQ(list.end(), list.begin());
}
void test_order_mixed(bool extended_lock, bool reversed) {
  if (extended_lock) {
    test_order_mixed_impl< McsRwExtendedBlock >(reversed);
  } else {
    test_order_mixed_impl< McsRwSimpleBlock >(reversed);
  }
}

/**
 * Really tests the behavior of unconditional/try locks with multi threads.
 * As the above "mixed" unit test covers page-vs-record handling,
 * this test only focuses on record locks.
 */
template<typename RW_BLOCK>
class TestConflicts {
 public:
  enum Constants {
    kMaxLockCount = 10U,
    kTestRepeats = 5U,
    kMaxThreads = 2U,  // so far simple 2-thread test only.
    kMainThreadId = 0,
    kSubThreadId = 1,
  };

  std::atomic<int> wait_on_;
  RwLockableXctId* lock_addresses_[kMaxLockCount];
  UniversalLockId lock_ids_[kMaxLockCount];
  storage::VolatilePagePointer page_ids_[kMaxLockCount];
  McsMockContext< RW_BLOCK > con_;

  TestConflicts() {
    con_.init(kDummyStorageId, kNodes, kMaxThreads, 1U << 16, kMaxLockCount);
    wait_on_ = 0;
    for (uint32_t i = 0; i < kMaxLockCount; ++i) {
      lock_addresses_[i] = con_.get_rw_lock_address(kDefaultNodeId, i);
      lock_addresses_[i]->xct_id_.set_epoch_int(i + 1U);
      page_ids_[i] = storage::to_page(lock_addresses_[i])->get_volatile_page_id();
      lock_ids_[i] = to_universal_lock_id(
        page_ids_[i].get_numa_node(),
        page_ids_[i].get_offset(),
        reinterpret_cast<uintptr_t>(lock_addresses_[i]));
    }
  }

  void wait_for_value(int value) {
    assorted::spin_until([this, value](){ return this->wait_on_.load() == value; });
  }
  void signal_value(int value) {
    wait_on_.store(value);
  }

  uint32_t get_repeat_times() const {
    if (assorted::is_running_on_valgrind()) {
      return 1U;
    } else {
      return kTestRepeats;
    }
  }
  void sleep_long_enough() const {
    if (assorted::is_running_on_valgrind()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  /**
   * Main thread: lock 3. launch Subthread and sleep for 10ms.
   * Subthread (no enclosing_max_id): lock 1, lock 2
   * Subthread: lock 3 unconditionally. wait....
   * Main thread: unlock 3.
   * Subthread: locked 3. lock 4.
   */
  void unconditional_livelock() {
    for (uint32_t i = 0; i < get_repeat_times(); ++i) {
      LOG(INFO) << " ======= unconditional_livelock test ======== ";
      unconditional_livelock_main();
    }
  }

  void unconditional_livelock_main() {
    signal_value(0);
    SysxctLockList list;
    list.init();

    McsMockAdaptor< RW_BLOCK > adaptor(kMainThreadId, &con_);
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[3], lock_addresses_[3]));
    LOG(INFO) << "Main thread: Locked 3";

    LOG(INFO) << "Main thread: Launching subthread...";
    std::thread t(&TestConflicts::unconditional_livelock_sub, this);
    sleep_long_enough();

    LOG(INFO) << "Main thread: woke up! will release all";
    list.release_all_locks(adaptor);

    t.join();
    LOG(INFO) << "Main thread: Joined subthread";
    for (uint32_t i = 0; i < kMaxLockCount; ++i) {
      EXPECT_FALSE(lock_addresses_[i]->is_keylocked());
    }
    LOG(INFO) << "Main thread: All done";
  }

  void unconditional_livelock_sub() {
    LOG(INFO) << "Subthread: starts";
    SysxctLockList list;
    list.init();

    McsMockAdaptor< RW_BLOCK > adaptor(kSubThreadId, &con_);
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[1], lock_addresses_[1]));
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[2], lock_addresses_[2]));

    LOG(INFO) << "Subthread: Now will wait for 3..";
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[3], lock_addresses_[3]));
    LOG(INFO) << "Subthread: locked!";
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[4], lock_addresses_[4]));

    LOG(INFO) << "Subthread: releasing";
    list.release_all_locks(adaptor);
    LOG(INFO) << "Subthread: done";
  }

  /**
   * Main takes 3
   * Subthread takes 5, 4, then 3.
   * Thus subthread will take it in try mode, and 3 will fail.
   * Subthread will keep trying until main thread wakes up and releases locks.
   */
  void try_livelock() {
    for (uint32_t i = 0; i < get_repeat_times(); ++i) {
      LOG(INFO) << " ======= try_livelock test ======== ";
      try_livelock_main();
    }
  }

  void try_livelock_main() {
    signal_value(0);
    SysxctLockList list;
    list.init();

    McsMockAdaptor< RW_BLOCK > adaptor(kMainThreadId, &con_);
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[3], lock_addresses_[3]));
    LOG(INFO) << "Main thread: Locked 3";

    LOG(INFO) << "Main thread: Launching subthread...";
    std::thread t(&TestConflicts::try_livelock_sub, this);
    sleep_long_enough();

    LOG(INFO) << "Main thread: woke up! will release all";
    list.release_all_locks(adaptor);

    t.join();
    LOG(INFO) << "Main thread: Joined subthread";
    for (uint32_t i = 0; i < kMaxLockCount; ++i) {
      EXPECT_FALSE(lock_addresses_[i]->is_keylocked());
    }
    LOG(INFO) << "Main thread: All done";
  }

  void try_livelock_sub() {
    LOG(INFO) << "Subthread: starts";
    SysxctLockList list;
    list.init();

    McsMockAdaptor< RW_BLOCK > adaptor(kSubThreadId, &con_);
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[5], lock_addresses_[5]));
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[4], lock_addresses_[4]));

    LOG(INFO) << "Subthread: Now will keep trying for 3..";
    uint32_t retries = 0;
    assorted::spin_until([&](){
      ErrorCode ret = list.request_record_lock(adaptor, page_ids_[3], lock_addresses_[3]);
      if (ret == kErrorCodeOk) {
        return true;
      }
      ++retries;
      return false;
    });
    LOG(INFO) << "Subthread: Finally took 3! Did " << retries << " retries";
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[2], lock_addresses_[2]));

    LOG(INFO) << "Subthread: releasing";
    list.release_all_locks(adaptor);
    LOG(INFO) << "Subthread: done";
  }


  /**
   * Main takes 3, then wait_for_value(1).
   * Subthread takes 4, then signal_value(1), then locks 3.
   * Main takes 4.
   * Keep doing it for 50ms (a LOOONG time).
   * Then subthread gives up and releases all.
   */
  void try_deadlock() {
    for (uint32_t i = 0; i < get_repeat_times(); ++i) {
      LOG(INFO) << " ======= try_deadlock test ======== ";
      try_deadlock_main();
    }
  }

  void try_deadlock_main() {
    signal_value(0);
    SysxctLockList list;
    list.init();

    McsMockAdaptor< RW_BLOCK > adaptor(kMainThreadId, &con_);
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[3], lock_addresses_[3]));
    LOG(INFO) << "Main thread: Locked 3";

    LOG(INFO) << "Main thread: Launching subthread...";
    std::thread t(&TestConflicts::try_deadlock_sub, this);
    wait_for_value(1);

    LOG(INFO) << "Main thread: woke up! Taking 4...";
    debugging::StopWatch watch;
    watch.start();
    uint32_t retries = 0;
    assorted::spin_until([&](){
      ErrorCode ret = list.request_record_lock(adaptor, page_ids_[4], lock_addresses_[4]);
      ++retries;
      return ret == kErrorCodeOk;
    });
    LOG(INFO) << "Main thread: Tried " << retries << " times. Finally taken";

    list.release_all_locks(adaptor);

    t.join();
    LOG(INFO) << "Main thread: Joined subthread";
    for (uint32_t i = 0; i < kMaxLockCount; ++i) {
      EXPECT_FALSE(lock_addresses_[i]->is_keylocked());
    }
    LOG(INFO) << "Main thread: All done";
  }

  void try_deadlock_sub() {
    LOG(INFO) << "Subthread: starts";
    SysxctLockList list;
    list.init();

    McsMockAdaptor< RW_BLOCK > adaptor(kSubThreadId, &con_);
    EXPECT_EQ(kErrorCodeOk, list.request_record_lock(adaptor, page_ids_[4], lock_addresses_[4]));

    LOG(INFO) << "Subthread: Took 4. Signaling main thread..";
    signal_value(1);

    LOG(INFO) << "Subthread: Now will keep trying for 3..";
    debugging::StopWatch watch;
    watch.start();
    uint32_t retries = 0;
    assorted::spin_until([&](){
      ErrorCode ret = list.request_record_lock(adaptor, page_ids_[3], lock_addresses_[3]);
      EXPECT_EQ(kErrorCodeXctRaceAbort, ret);
      ++retries;
      return (watch.peek_elapsed_ns() >= 50000000UL);
    });

    LOG(INFO) << "Subthread: Gave up taking 3! Did " << retries << " retries";
    LOG(INFO) << "Subthread: releasing";
    list.release_all_locks(adaptor);
    LOG(INFO) << "Subthread: done";
  }
};


TEST(SysxctLockListTest, InOrderRecordSimple)         { test_order_record(false, false); }
TEST(SysxctLockListTest, InOrderRecordExtended)       { test_order_record(true,  false); }
TEST(SysxctLockListTest, ReverseOrderRecordSimple)    { test_order_record(false, true); }
TEST(SysxctLockListTest, ReverseOrderRecordExtended)  { test_order_record(true,  true); }


TEST(SysxctLockListTest, IsTryRequiredRecord)         { TestIsTryRequired().run_record(); }

TEST(SysxctLockListTest, InOrderMixedSimple)          { test_order_mixed(false, false); }
TEST(SysxctLockListTest, InOrderMixedExtended)        { test_order_mixed(true,  false); }
TEST(SysxctLockListTest, ReverseOrderMixedSimple)     { test_order_mixed(false, true); }
TEST(SysxctLockListTest, ReverseOrderMixedExtended)   { test_order_mixed(true,  true); }

typedef TestConflicts<McsRwSimpleBlock>   SimpleTest;
typedef TestConflicts<McsRwExtendedBlock> ExtendTest;
TEST(SysxctLockListTest, UnconditionalLivelockSimple)   { SimpleTest().unconditional_livelock(); }
TEST(SysxctLockListTest, UnconditionalLivelockExtended) { ExtendTest().unconditional_livelock(); }
TEST(SysxctLockListTest, TryLivelockSimple)             { SimpleTest().try_livelock(); }
TEST(SysxctLockListTest, TryLivelockExtended)           { ExtendTest().try_livelock(); }
TEST(SysxctLockListTest, TryDeadlockSimple)             { SimpleTest().try_deadlock(); }
TEST(SysxctLockListTest, TryDeadlockExtended)           { ExtendTest().try_deadlock(); }

}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SysxctLockListTest, foedus.xct);
