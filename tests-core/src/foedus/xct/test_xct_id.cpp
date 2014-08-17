/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <gtest/gtest.h>

#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(XctIdTest, foedus.xct);


TEST(XctIdTest, Empty) {
  XctId id;
  EXPECT_FALSE(id.is_valid());
  EXPECT_EQ(0, id.get_epoch_int());
  EXPECT_EQ(0, id.get_ordinal());
}
TEST(XctIdTest, SetAll) {
  XctId id;
  id.set(123, 456);
  EXPECT_TRUE(id.is_valid());
  EXPECT_EQ(123, id.get_epoch_int());
  EXPECT_EQ(456, id.get_ordinal());
}

TEST(XctIdTest, SetEpoch) {
  XctId id;
  id.set(123, 456);
  id.set_epoch_int(997);
  EXPECT_TRUE(id.is_valid());
  EXPECT_EQ(997, id.get_epoch_int());
  EXPECT_EQ(456, id.get_ordinal());
  id.set_epoch(Epoch(8875));
  EXPECT_EQ(8875, id.get_epoch_int());
  EXPECT_EQ(456, id.get_ordinal());
}

TEST(XctIdTest, SetOrdinal) {
  XctId id;
  id.set(123, 456);
  id.set_ordinal(5423);
  EXPECT_TRUE(id.is_valid());
  EXPECT_EQ(123, id.get_epoch_int());
  EXPECT_EQ(5423, id.get_ordinal());
}

TEST(XctIdTest, SetThread) {
  XctId id;
  id.set(123, 456);
  EXPECT_TRUE(id.is_valid());
  EXPECT_EQ(123, id.get_epoch_int());
  EXPECT_EQ(456, id.get_ordinal());
}

}  // namespace xct
}  // namespace foedus
