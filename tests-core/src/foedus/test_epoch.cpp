/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/epoch.hpp>
#include <gtest/gtest.h>
#include <iostream>

namespace foedus {
DEFINE_TEST_CASE_PACKAGE(EpochTest, foedus);

TEST(EpochTest, Valid) {
  Epoch valid_epoch(123);
  EXPECT_TRUE(valid_epoch.is_valid());
  Epoch invalid_epoch;
  EXPECT_FALSE(invalid_epoch.is_valid());
}

TEST(EpochTest, Comparison) {
  Epoch ep1(123);
  Epoch ep2(123);
  Epoch ep3(124);

  EXPECT_TRUE(ep1 == ep2);
  EXPECT_TRUE(ep1 <= ep2);
  EXPECT_TRUE(ep1 >= ep2);
  EXPECT_FALSE(ep1 != ep2);
  EXPECT_FALSE(ep1 > ep2);
  EXPECT_FALSE(ep1 < ep2);

  EXPECT_FALSE(ep1 == ep3);
  EXPECT_TRUE(ep1 <= ep3);
  EXPECT_FALSE(ep1 >= ep3);
  EXPECT_TRUE(ep1 != ep3);
  EXPECT_FALSE(ep1 > ep3);
  EXPECT_TRUE(ep1 < ep3);
}

}  // namespace foedus
