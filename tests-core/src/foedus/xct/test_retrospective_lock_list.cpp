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

#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/xct/retrospective_lock_list.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_manager_pimpl.hpp"  // For CurrentLockListIteratorForWriteSet
#include "foedus/xct/xct_mcs_adapter_impl.hpp"

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(RllTest, foedus.xct);

const uint16_t kNodes = 1U;
const uint32_t kMaxLockCount = 16;
const uint16_t kDummyStorageId = 1U;
const uint16_t kDefaultNodeId = 0U;

TEST(RllTest, CllAddSearch) {
  RwLockableXctId* lock_addresses[kMaxLockCount];
  UniversalLockId lock_ids[kMaxLockCount];
  McsMockContext<McsRwSimpleBlock> con;
  con.init(kDummyStorageId, kNodes, 1U, 1U << 16, kMaxLockCount);
  for (uint32_t i = 0; i < kMaxLockCount; ++i) {
    lock_addresses[i] = con.get_rw_lock_address(kDefaultNodeId, i);
    lock_addresses[i]->xct_id_.set_epoch_int(i + 1U);
    lock_ids[i] = xct::to_universal_lock_id(
      con.page_memory_resolver_,
      reinterpret_cast<uintptr_t>(lock_addresses[i]));
  }

  const uint32_t kBufferSize = 1024;
  LockEntry cll_buffer[kBufferSize];
  CurrentLockList list;
  list.init(cll_buffer, kBufferSize, con.page_memory_resolver_);
  EXPECT_EQ(kBufferSize, list.get_capacity());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());

  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[2]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[3]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[5]));

  // Add 3 -> [3]
  EXPECT_EQ(1U, list.get_or_add_entry(lock_addresses[3], kReadLock));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[2]));
  EXPECT_EQ(1U, list.binary_search(lock_ids[3]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[5]));
  EXPECT_EQ(1U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  // Add 5 -> [3, 5]
  EXPECT_EQ(2U, list.get_or_add_entry(lock_addresses[5], kReadLock));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[2]));
  EXPECT_EQ(1U, list.binary_search(lock_ids[3]));
  EXPECT_EQ(2U, list.binary_search(lock_ids[5]));
  EXPECT_EQ(2U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  // Add 2 -> [2, 3, 5]
  EXPECT_EQ(1U, list.get_or_add_entry(lock_addresses[2], kReadLock));
  EXPECT_EQ(1U, list.binary_search(lock_ids[2]));
  EXPECT_EQ(2U, list.binary_search(lock_ids[3]));
  EXPECT_EQ(3U, list.binary_search(lock_ids[5]));
  EXPECT_EQ(3U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  // Add 3 (ignored) -> [2, 3, 5]
  EXPECT_EQ(2U, list.get_or_add_entry(lock_addresses[3], kReadLock));
  EXPECT_EQ(1U, list.binary_search(lock_ids[2]));
  EXPECT_EQ(2U, list.binary_search(lock_ids[3]));
  EXPECT_EQ(3U, list.binary_search(lock_ids[5]));
  EXPECT_EQ(3U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  EXPECT_EQ(1U, list.lower_bound(lock_ids[1]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[1]));
  EXPECT_EQ(1U, list.lower_bound(lock_ids[2]));
  EXPECT_EQ(1U, list.binary_search(lock_ids[2]));
  EXPECT_EQ(2U, list.lower_bound(lock_ids[3]));
  EXPECT_EQ(2U, list.binary_search(lock_ids[3]));
  EXPECT_EQ(3U, list.lower_bound(lock_ids[4]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[4]));
  EXPECT_EQ(3U, list.lower_bound(lock_ids[5]));
  EXPECT_EQ(3U, list.binary_search(lock_ids[5]));
  EXPECT_EQ(4U, list.lower_bound(lock_ids[6]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[6]));
  EXPECT_EQ(4U, list.lower_bound(lock_ids[7]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[7]));
}

TEST(RllTest, CllBatchInsertFromEmpty) {
  RwLockableXctId* lock_addresses[kMaxLockCount];
  UniversalLockId lock_ids[kMaxLockCount];
  McsMockContext<McsRwSimpleBlock> con;
  con.init(kDummyStorageId, kNodes, 1U, 1U << 16, kMaxLockCount);
  for (uint32_t i = 0; i < kMaxLockCount; ++i) {
    lock_addresses[i] = con.get_rw_lock_address(kDefaultNodeId, i);
    lock_addresses[i]->xct_id_.set_epoch_int(i + 1U);
    lock_ids[i] = xct::to_universal_lock_id(
      con.page_memory_resolver_,
      reinterpret_cast<uintptr_t>(lock_addresses[i]));
  }

  const uint32_t kBufferSize = 1024;
  LockEntry cll_buffer[kBufferSize];
  CurrentLockList list;
  list.init(cll_buffer, kBufferSize, con.page_memory_resolver_);

  const uint32_t kWriteSetSize = 4;
  WriteXctAccess write_set[kWriteSetSize];
  write_set[0].write_set_ordinal_ = 0;
  write_set[0].owner_id_address_ = lock_addresses[5];
  write_set[0].owner_lock_id_ = lock_ids[5];
  write_set[1].write_set_ordinal_ = 1;
  write_set[1].owner_id_address_ = lock_addresses[3];
  write_set[1].owner_lock_id_ = lock_ids[3];
  write_set[2].write_set_ordinal_ = 2;
  write_set[2].owner_id_address_ = lock_addresses[9];
  write_set[2].owner_lock_id_ = lock_ids[9];
  write_set[3].write_set_ordinal_ = 3;
  write_set[3].owner_id_address_ = lock_addresses[5];
  write_set[3].owner_lock_id_ = lock_ids[5];
  std::sort(write_set, write_set + kWriteSetSize, WriteXctAccess::compare);

  list.batch_insert_write_placeholders(write_set, kWriteSetSize);
  EXPECT_EQ(3U, list.get_last_active_entry());

  EXPECT_EQ(1U, list.lower_bound(lock_ids[1]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[1]));
  EXPECT_EQ(1U, list.lower_bound(lock_ids[2]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[2]));
  EXPECT_EQ(1U, list.lower_bound(lock_ids[3]));
  EXPECT_EQ(1U, list.binary_search(lock_ids[3]));
  EXPECT_EQ(2U, list.lower_bound(lock_ids[4]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[4]));
  EXPECT_EQ(2U, list.lower_bound(lock_ids[5]));
  EXPECT_EQ(2U, list.binary_search(lock_ids[5]));
  EXPECT_EQ(3U, list.lower_bound(lock_ids[6]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[6]));
  EXPECT_EQ(3U, list.lower_bound(lock_ids[8]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[8]));
  EXPECT_EQ(3U, list.lower_bound(lock_ids[9]));
  EXPECT_EQ(3U, list.binary_search(lock_ids[9]));
  EXPECT_EQ(4U, list.lower_bound(lock_ids[10]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[10]));

  // Also test CurrentLockListIteratorForWriteSet
  CurrentLockListIteratorForWriteSet it(write_set, &list, kWriteSetSize);
  // 3
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(1U, it.cll_pos_);
  EXPECT_EQ(0U, it.write_cur_pos_);
  EXPECT_EQ(1U, it.write_next_pos_);

  // 5,5
  it.next_writes();
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(2U, it.cll_pos_);
  EXPECT_EQ(1U, it.write_cur_pos_);
  EXPECT_EQ(3U, it.write_next_pos_);

  // 9
  it.next_writes();
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(3U, it.cll_pos_);
  EXPECT_EQ(3U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);

  // Done
  it.next_writes();
  EXPECT_FALSE(it.is_valid());
  EXPECT_EQ(4U, it.cll_pos_);
  EXPECT_EQ(4U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);
}

TEST(RllTest, CllBatchInsertMerge) {
  RwLockableXctId* lock_addresses[kMaxLockCount];
  UniversalLockId lock_ids[kMaxLockCount];
  McsMockContext<McsRwSimpleBlock> con;
  con.init(kDummyStorageId, kNodes, 1U, 1U << 16, kMaxLockCount);
  for (uint32_t i = 0; i < kMaxLockCount; ++i) {
    lock_addresses[i] = con.get_rw_lock_address(kDefaultNodeId, i);
    lock_addresses[i]->xct_id_.set_epoch_int(i + 1U);
    lock_ids[i] = xct::to_universal_lock_id(
      con.page_memory_resolver_,
      reinterpret_cast<uintptr_t>(lock_addresses[i]));
  }

  const uint32_t kBufferSize = 1024;
  LockEntry cll_buffer[kBufferSize];
  CurrentLockList list;
  list.init(cll_buffer, kBufferSize, con.page_memory_resolver_);

  // Populate with [2, 6, 9]
  list.get_or_add_entry(lock_addresses[2], kReadLock);
  list.get_or_add_entry(lock_addresses[6], kReadLock);
  list.get_or_add_entry(lock_addresses[9], kReadLock);

  const uint32_t kWriteSetSize = 4;
  WriteXctAccess write_set[kWriteSetSize];
  write_set[0].write_set_ordinal_ = 0;
  write_set[0].owner_id_address_ = lock_addresses[5];
  write_set[0].owner_lock_id_ = lock_ids[5];
  write_set[1].write_set_ordinal_ = 1;
  write_set[1].owner_id_address_ = lock_addresses[3];
  write_set[1].owner_lock_id_ = lock_ids[3];
  write_set[2].write_set_ordinal_ = 2;
  write_set[2].owner_id_address_ = lock_addresses[9];
  write_set[2].owner_lock_id_ = lock_ids[9];
  write_set[3].write_set_ordinal_ = 3;
  write_set[3].owner_id_address_ = lock_addresses[5];
  write_set[3].owner_lock_id_ = lock_ids[5];
  std::sort(write_set, write_set + kWriteSetSize, WriteXctAccess::compare);

  list.batch_insert_write_placeholders(write_set, kWriteSetSize);
  // Now it should be [2, 3, 5, 6, 9]
  EXPECT_EQ(5U, list.get_last_active_entry());

  EXPECT_EQ(1U, list.lower_bound(lock_ids[1]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[1]));
  EXPECT_EQ(1U, list.lower_bound(lock_ids[2]));
  EXPECT_EQ(1U, list.binary_search(lock_ids[2]));
  EXPECT_EQ(2U, list.lower_bound(lock_ids[3]));
  EXPECT_EQ(2U, list.binary_search(lock_ids[3]));
  EXPECT_EQ(3U, list.lower_bound(lock_ids[4]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[4]));
  EXPECT_EQ(3U, list.lower_bound(lock_ids[5]));
  EXPECT_EQ(3U, list.binary_search(lock_ids[5]));
  EXPECT_EQ(4U, list.lower_bound(lock_ids[6]));
  EXPECT_EQ(4U, list.binary_search(lock_ids[6]));
  EXPECT_EQ(5U, list.lower_bound(lock_ids[7]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[7]));
  EXPECT_EQ(5U, list.lower_bound(lock_ids[8]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[8]));
  EXPECT_EQ(5U, list.lower_bound(lock_ids[9]));
  EXPECT_EQ(5U, list.binary_search(lock_ids[9]));
  EXPECT_EQ(6U, list.lower_bound(lock_ids[10]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(lock_ids[10]));

  // Also test CurrentLockListIteratorForWriteSet
  CurrentLockListIteratorForWriteSet it(write_set, &list, kWriteSetSize);
  // 3 (skips "2")
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(2U, it.cll_pos_);
  EXPECT_EQ(0U, it.write_cur_pos_);
  EXPECT_EQ(1U, it.write_next_pos_);

  // 5,5
  it.next_writes();
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(3U, it.cll_pos_);
  EXPECT_EQ(1U, it.write_cur_pos_);
  EXPECT_EQ(3U, it.write_next_pos_);

  // 9 (skips "6")
  it.next_writes();
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(5U, it.cll_pos_);
  EXPECT_EQ(3U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);

  // Done
  it.next_writes();
  EXPECT_FALSE(it.is_valid());
  EXPECT_EQ(6U, it.cll_pos_);
  EXPECT_EQ(4U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);
}

}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(RllTest, foedus.xct);
