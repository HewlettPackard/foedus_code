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

#include <stdint.h>

#include "foedus/test_common.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/assorted/uniform_random.hpp"

namespace foedus {
namespace assorted {

void test_internal32(uint32_t n, uint32_t d) {
  ConstDiv div(d);
  uint32_t ret = div.div32(n);
  EXPECT_EQ(n / d, ret) << n << "/" << d;
}
void test_internal64(uint64_t n, uint32_t d) {
  ConstDiv div(d);
  uint64_t ret = div.div64(n);
  EXPECT_EQ(n / d, ret) << n << "/" << d;
}

void test(uint32_t d) {
  for (uint32_t i = 0; i < 13; ++i) {
    test_internal32(i * 256, d);
    test_internal64((static_cast<uint64_t>(i * 256) << 32) + i * 123, d);
  }
  test_internal32(12345778, d);
  test_internal64(0xADF4251098234523ULL, d);

  UniformRandom rnd(123L);
  for (int i = 0; i < 100; ++i) {
    test_internal32(rnd.next_uint32(), d);
    test_internal64(rnd.next_uint64(), d);
  }
}

DEFINE_TEST_CASE_PACKAGE(ConstDivTest, foedus.assorted);
TEST(ConstDivTest, Test3) { test(3); }
TEST(ConstDivTest, Test4) { test(4); }
TEST(ConstDivTest, Test5) { test(5); }
TEST(ConstDivTest, Test6) { test(6); }
TEST(ConstDivTest, Test7) { test(7); }
TEST(ConstDivTest, Test8) { test(8); }
TEST(ConstDivTest, Test9) { test(9); }
TEST(ConstDivTest, Test10) { test(10); }
TEST(ConstDivTest, Test11) { test(11); }
TEST(ConstDivTest, Test12) { test(12); }
TEST(ConstDivTest, Test25) { test(25); }
TEST(ConstDivTest, Test60) { test(60); }
TEST(ConstDivTest, Test125) { test(125); }
TEST(ConstDivTest, Test254) { test(254); }
TEST(ConstDivTest, Test256) { test(256); }
TEST(ConstDivTest, Test625) { test(625); }
TEST(ConstDivTest, TestRandom) {
  UniformRandom rnd(123L);
  for (int i = 0; i < 100; ++i) {
    test_internal32(rnd.next_uint32(), rnd.next_uint32());
    test_internal64(rnd.next_uint64(), rnd.next_uint32());
  }
}
}  // namespace assorted
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(ConstDivTest, foedus.assorted);
