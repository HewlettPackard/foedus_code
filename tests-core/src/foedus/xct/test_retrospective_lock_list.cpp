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

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(RllTest, foedus.xct);

TEST(RllTest, CllAddSearch) {
  const uint32_t kBufferSize = 1024;
  LockEntry cll_buffer[kBufferSize];
  CurrentLockList list;
  list.init(cll_buffer, kBufferSize);
  EXPECT_EQ(kBufferSize, list.get_capacity());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());

  const uint32_t kLockCount = 16;
  RwLockableXctId lock_objects[kLockCount];
  RwLockableXctId* dummy_locks[kLockCount];
  std::memset(lock_objects, 0, sizeof(RwLockableXctId) * kLockCount);
  for (uint32_t i = 0; i < kLockCount; ++i) {
    lock_objects[i].xct_id_.set_epoch_int(i + 1U);
    dummy_locks[i] = &lock_objects[i];
  }

  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[5]));

  // Add 3 -> [3]
  EXPECT_EQ(1U, list.get_or_add_entry(NULL, dummy_locks[3], kReadLock));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(1U, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[5]));
  EXPECT_EQ(1U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  // Add 5 -> [3, 5]
  EXPECT_EQ(2U, list.get_or_add_entry(NULL, dummy_locks[5], kReadLock));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(1U, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(2U, list.binary_search(NULL, dummy_locks[5]));
  EXPECT_EQ(2U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  // Add 2 -> [2, 3, 5]
  EXPECT_EQ(1U, list.get_or_add_entry(NULL, dummy_locks[2], kReadLock));
  EXPECT_EQ(1U, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(2U, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(3U, list.binary_search(NULL, dummy_locks[5]));
  EXPECT_EQ(3U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  // Add 3 (ignored) -> [2, 3, 5]
  EXPECT_EQ(2U, list.get_or_add_entry(NULL, dummy_locks[3], kReadLock));
  EXPECT_EQ(1U, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(2U, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(3U, list.binary_search(NULL, dummy_locks[5]));
  EXPECT_EQ(3U, list.get_last_active_entry());
  EXPECT_EQ(kLockListPositionInvalid, list.get_last_locked_entry());
  LOG(INFO) << list;

  EXPECT_EQ(1U, list.lower_bound(NULL, dummy_locks[1]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[1]));
  EXPECT_EQ(1U, list.lower_bound(NULL, dummy_locks[2]));
  EXPECT_EQ(1U, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(2U, list.lower_bound(NULL, dummy_locks[3]));
  EXPECT_EQ(2U, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(3U, list.lower_bound(NULL, dummy_locks[4]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[4]));
  EXPECT_EQ(3U, list.lower_bound(NULL, dummy_locks[5]));
  EXPECT_EQ(3U, list.binary_search(NULL, dummy_locks[5]));
  EXPECT_EQ(4U, list.lower_bound(NULL, dummy_locks[6]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[6]));
  EXPECT_EQ(4U, list.lower_bound(NULL, dummy_locks[7]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[7]));
}

TEST(RllTest, CllBatchInsertFromEmpty) {
  const uint32_t kBufferSize = 1024;
  LockEntry cll_buffer[kBufferSize];
  CurrentLockList list;
  list.init(cll_buffer, kBufferSize);

  const uint32_t kLockCount = 16;
  RwLockableXctId lock_objects[kLockCount];
  RwLockableXctId* dummy_locks[kLockCount];
  std::memset(lock_objects, 0, sizeof(RwLockableXctId) * kLockCount);
  for (uint32_t i = 0; i < kLockCount; ++i) {
    lock_objects[i].xct_id_.set_epoch_int(i + 1U);
    dummy_locks[i] = &lock_objects[i];
  }

  const uint32_t kWriteSetSize = 4;
  WriteXctAccess write_set[kWriteSetSize];
  write_set[0].write_set_ordinal_ = 0;
  write_set[0].owner_id_address_ = dummy_locks[5];
  write_set[1].write_set_ordinal_ = 1;
  write_set[1].owner_id_address_ = dummy_locks[3];
  write_set[2].write_set_ordinal_ = 2;
  write_set[2].owner_id_address_ = dummy_locks[9];
  write_set[3].write_set_ordinal_ = 3;
  write_set[3].owner_id_address_ = dummy_locks[5];
  std::sort(write_set, write_set + kWriteSetSize, WriteXctAccess::compare);

  list.batch_insert_write_placeholders(write_set, kWriteSetSize, NULL);
  EXPECT_EQ(3U, list.get_last_active_entry());

  EXPECT_EQ(1U, list.lower_bound(NULL, dummy_locks[1]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[1]));
  EXPECT_EQ(1U, list.lower_bound(NULL, dummy_locks[2]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(1U, list.lower_bound(NULL, dummy_locks[3]));
  EXPECT_EQ(1U, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(2U, list.lower_bound(NULL, dummy_locks[4]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[4]));
  EXPECT_EQ(2U, list.lower_bound(NULL, dummy_locks[5]));
  EXPECT_EQ(2U, list.binary_search(NULL, dummy_locks[5]));
  EXPECT_EQ(3U, list.lower_bound(NULL, dummy_locks[6]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[6]));
  EXPECT_EQ(3U, list.lower_bound(NULL, dummy_locks[8]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[8]));
  EXPECT_EQ(3U, list.lower_bound(NULL, dummy_locks[9]));
  EXPECT_EQ(3U, list.binary_search(NULL, dummy_locks[9]));
  EXPECT_EQ(4U, list.lower_bound(NULL, dummy_locks[10]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[10]));

  // Also test CurrentLockListIteratorForWriteSet
  CurrentLockListIteratorForWriteSet it(NULL, write_set, &list, kWriteSetSize);
  // 3
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(1U, it.cll_pos_);
  EXPECT_EQ(0U, it.write_cur_pos_);
  EXPECT_EQ(1U, it.write_next_pos_);

  // 5,5
  it.next_writes(NULL);
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(2U, it.cll_pos_);
  EXPECT_EQ(1U, it.write_cur_pos_);
  EXPECT_EQ(3U, it.write_next_pos_);

  // 9
  it.next_writes(NULL);
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(3U, it.cll_pos_);
  EXPECT_EQ(3U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);

  // Done
  it.next_writes(NULL);
  EXPECT_FALSE(it.is_valid());
  EXPECT_EQ(4U, it.cll_pos_);
  EXPECT_EQ(4U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);
}

TEST(RllTest, CllBatchInsertMerge) {
  const uint32_t kBufferSize = 1024;
  LockEntry cll_buffer[kBufferSize];
  CurrentLockList list;
  list.init(cll_buffer, kBufferSize);

  const uint32_t kLockCount = 16;
  RwLockableXctId lock_objects[kLockCount];
  RwLockableXctId* dummy_locks[kLockCount];
  std::memset(lock_objects, 0, sizeof(RwLockableXctId) * kLockCount);
  for (uint32_t i = 0; i < kLockCount; ++i) {
    lock_objects[i].xct_id_.set_epoch_int(i + 1U);
    dummy_locks[i] = &lock_objects[i];
  }

  // Populate with [2, 6, 9]
  list.get_or_add_entry(NULL, dummy_locks[2], kReadLock);
  list.get_or_add_entry(NULL, dummy_locks[6], kReadLock);
  list.get_or_add_entry(NULL, dummy_locks[9], kReadLock);

  const uint32_t kWriteSetSize = 4;
  WriteXctAccess write_set[kWriteSetSize];
  write_set[0].write_set_ordinal_ = 0;
  write_set[0].owner_id_address_ = dummy_locks[5];
  write_set[1].write_set_ordinal_ = 1;
  write_set[1].owner_id_address_ = dummy_locks[3];
  write_set[2].write_set_ordinal_ = 2;
  write_set[2].owner_id_address_ = dummy_locks[9];
  write_set[3].write_set_ordinal_ = 3;
  write_set[3].owner_id_address_ = dummy_locks[5];
  std::sort(write_set, write_set + kWriteSetSize, WriteXctAccess::compare);

  list.batch_insert_write_placeholders(write_set, kWriteSetSize, NULL);
  // Now it should be [2, 3, 5, 6, 9]
  EXPECT_EQ(5U, list.get_last_active_entry());

  EXPECT_EQ(1U, list.lower_bound(NULL, dummy_locks[1]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[1]));
  EXPECT_EQ(1U, list.lower_bound(NULL, dummy_locks[2]));
  EXPECT_EQ(1U, list.binary_search(NULL, dummy_locks[2]));
  EXPECT_EQ(2U, list.lower_bound(NULL, dummy_locks[3]));
  EXPECT_EQ(2U, list.binary_search(NULL, dummy_locks[3]));
  EXPECT_EQ(3U, list.lower_bound(NULL, dummy_locks[4]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[4]));
  EXPECT_EQ(3U, list.lower_bound(NULL, dummy_locks[5]));
  EXPECT_EQ(3U, list.binary_search(NULL, dummy_locks[5]));
  EXPECT_EQ(4U, list.lower_bound(NULL, dummy_locks[6]));
  EXPECT_EQ(4U, list.binary_search(NULL, dummy_locks[6]));
  EXPECT_EQ(5U, list.lower_bound(NULL, dummy_locks[7]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[7]));
  EXPECT_EQ(5U, list.lower_bound(NULL, dummy_locks[8]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[8]));
  EXPECT_EQ(5U, list.lower_bound(NULL, dummy_locks[9]));
  EXPECT_EQ(5U, list.binary_search(NULL, dummy_locks[9]));
  EXPECT_EQ(6U, list.lower_bound(NULL, dummy_locks[10]));
  EXPECT_EQ(kLockListPositionInvalid, list.binary_search(NULL, dummy_locks[10]));

  // Also test CurrentLockListIteratorForWriteSet
  CurrentLockListIteratorForWriteSet it(NULL, write_set, &list, kWriteSetSize);
  // 3 (skips "2")
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(2U, it.cll_pos_);
  EXPECT_EQ(0U, it.write_cur_pos_);
  EXPECT_EQ(1U, it.write_next_pos_);

  // 5,5
  it.next_writes(NULL);
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(3U, it.cll_pos_);
  EXPECT_EQ(1U, it.write_cur_pos_);
  EXPECT_EQ(3U, it.write_next_pos_);

  // 9 (skips "6")
  it.next_writes(NULL);
  EXPECT_TRUE(it.is_valid());
  EXPECT_EQ(5U, it.cll_pos_);
  EXPECT_EQ(3U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);

  // Done
  it.next_writes(NULL);
  EXPECT_FALSE(it.is_valid());
  EXPECT_EQ(6U, it.cll_pos_);
  EXPECT_EQ(4U, it.write_cur_pos_);
  EXPECT_EQ(4U, it.write_next_pos_);
}

}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(RllTest, foedus.xct);
