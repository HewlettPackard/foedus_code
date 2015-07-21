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

TEST_MAIN_CAPTURE_SIGNALS(XctIdTest, foedus.xct);
