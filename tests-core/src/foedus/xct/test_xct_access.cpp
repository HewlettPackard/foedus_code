/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>

#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(XctAccessTest, foedus.xct);

void* to_ptr(int val) {
  return reinterpret_cast<void*>(static_cast<uintptr_t>(val));
}

XctAccess create_access(int i) {
  XctAccess access;
  access.observed_owner_id_.set_clean(i * 20, i * 12, i + 3);
  access.storage_ = reinterpret_cast<storage::Storage*>(to_ptr(i * 1234));
  access.owner_id_address_ = reinterpret_cast<xct::XctId*>(to_ptr(i * 8452));
  return access;
}
void verify_access(const XctAccess &access, int i) {
  XctId tmp;
  tmp.set_clean(i * 20, i * 12, i + 3);
  EXPECT_TRUE(access.observed_owner_id_.equals_all(tmp));
  EXPECT_TRUE(access.storage_ == reinterpret_cast<storage::Storage*>(to_ptr(i * 1234)));
  EXPECT_TRUE(access.owner_id_address_ == reinterpret_cast<xct::XctId*>(to_ptr(i * 8452)));
}

TEST(XctAccessTest, CompareReadSet) {
  XctAccess set1 = create_access(3);
  XctAccess set2 = create_access(4);

  EXPECT_TRUE(XctAccess::compare(set1, set2));
  EXPECT_FALSE(XctAccess::compare(set2, set1));
  EXPECT_FALSE(XctAccess::compare(set1, set1));
  EXPECT_FALSE(XctAccess::compare(set2, set2));

  set2 = create_access(2);
  EXPECT_FALSE(XctAccess::compare(set1, set2));
  EXPECT_TRUE(XctAccess::compare(set2, set1));
  EXPECT_FALSE(XctAccess::compare(set1, set1));
  EXPECT_FALSE(XctAccess::compare(set2, set2));
}

TEST(XctAccessTest, SortReadSet) {
  XctAccess sets[7];
  sets[0] = create_access(19);
  sets[1] = create_access(4);
  sets[2] = create_access(7);
  sets[3] = create_access(40);
  sets[4] = create_access(9);
  sets[5] = create_access(20);
  sets[6] = create_access(11);
  verify_access(sets[0], 19);
  verify_access(sets[1], 4);
  verify_access(sets[2], 7);
  verify_access(sets[3], 40);
  verify_access(sets[4], 9);
  verify_access(sets[5], 20);
  verify_access(sets[6], 11);

  std::sort(sets, sets + 7, XctAccess::compare);
  verify_access(sets[0], 4);
  verify_access(sets[1], 7);
  verify_access(sets[2], 9);
  verify_access(sets[3], 11);
  verify_access(sets[4], 19);
  verify_access(sets[5], 20);
  verify_access(sets[6], 40);
}

TEST(XctAccessTest, RandomReadSet) {
  const int kSize = 200;
  const int kSwapCount = 400;
  XctAccess sets[kSize];
  for (int i = 0; i < kSize; ++i) {
    sets[i] = create_access(i + 12);
  }
  assorted::UniformRandom rnd(1234);
  for (int i = 0; i < kSwapCount; ++i) {
    std::swap(sets[rnd.uniform_within(0, kSize - 1)], sets[rnd.uniform_within(0, kSize - 1)]);
  }
  std::sort(sets, sets + kSize, XctAccess::compare);
  for (int i = 0; i < kSize; ++i) {
    verify_access(sets[i], i + 12);
  }
}


WriteXctAccess create_write_access(int i) {
  WriteXctAccess access;
  access.payload_address_ = reinterpret_cast<char*>(to_ptr(i * 542312));
  access.storage_ = reinterpret_cast<storage::Storage*>(to_ptr(i * 52223));
  access.owner_id_address_ = reinterpret_cast<xct::XctId*>(to_ptr(i * 14325));
  access.log_entry_ = reinterpret_cast<log::RecordLogType*>(to_ptr(i * 5423423));
  return access;
}
void verify_access(const WriteXctAccess &access, int i) {
  XctId tmp;
  tmp.set_clean(i * 43, i * 4, i + 1);
  EXPECT_TRUE(access.payload_address_  == reinterpret_cast<char*>(to_ptr(i * 542312)));
  EXPECT_TRUE(access.storage_ == reinterpret_cast<storage::Storage*>(to_ptr(i * 52223)));
  EXPECT_TRUE(access.owner_id_address_ == reinterpret_cast<xct::XctId*>(to_ptr(i * 14325)));
  EXPECT_TRUE(access.log_entry_ == to_ptr(i * 5423423));
}

TEST(XctAccessTest, CompareWriteSet) {
  WriteXctAccess set1 = create_write_access(3);
  WriteXctAccess set2 = create_write_access(4);

  EXPECT_TRUE(WriteXctAccess::compare(set1, set2));
  EXPECT_FALSE(WriteXctAccess::compare(set2, set1));
  EXPECT_FALSE(WriteXctAccess::compare(set1, set1));
  EXPECT_FALSE(WriteXctAccess::compare(set2, set2));

  set2 = create_write_access(2);
  EXPECT_FALSE(WriteXctAccess::compare(set1, set2));
  EXPECT_TRUE(WriteXctAccess::compare(set2, set1));
  EXPECT_FALSE(WriteXctAccess::compare(set1, set1));
  EXPECT_FALSE(WriteXctAccess::compare(set2, set2));
}

TEST(XctAccessTest, SortWriteSet) {
  WriteXctAccess sets[7];
  sets[0] = create_write_access(19);
  sets[1] = create_write_access(4);
  sets[2] = create_write_access(7);
  sets[3] = create_write_access(40);
  sets[4] = create_write_access(9);
  sets[5] = create_write_access(20);
  sets[6] = create_write_access(11);
  verify_access(sets[0], 19);
  verify_access(sets[1], 4);
  verify_access(sets[2], 7);
  verify_access(sets[3], 40);
  verify_access(sets[4], 9);
  verify_access(sets[5], 20);
  verify_access(sets[6], 11);

  std::sort(sets, sets + 7, WriteXctAccess::compare);
  verify_access(sets[0], 4);
  verify_access(sets[1], 7);
  verify_access(sets[2], 9);
  verify_access(sets[3], 11);
  verify_access(sets[4], 19);
  verify_access(sets[5], 20);
  verify_access(sets[6], 40);
}

TEST(XctAccessTest, RandomWriteSet) {
  const int kSize = 200;
  const int kSwapCount = 400;
  WriteXctAccess sets[kSize];
  for (int i = 0; i < kSize; ++i) {
    sets[i] = create_write_access(i + 12);
  }
  assorted::UniformRandom rnd(1234);
  for (int i = 0; i < kSwapCount; ++i) {
    std::swap(sets[rnd.uniform_within(0, kSize - 1)], sets[rnd.uniform_within(0, kSize - 1)]);
  }
  std::sort(sets, sets + kSize, WriteXctAccess::compare);
  for (int i = 0; i < kSize; ++i) {
    verify_access(sets[i], i + 12);
  }
}
}  // namespace xct
}  // namespace foedus
