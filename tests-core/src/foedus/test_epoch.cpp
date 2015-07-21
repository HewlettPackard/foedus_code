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
#include <gtest/gtest.h>

#include <iostream>

#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"

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

TEST_MAIN_CAPTURE_SIGNALS(EpochTest, foedus);
