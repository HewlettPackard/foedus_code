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

#include <string>

#include "foedus/test_common.hpp"
#include "foedus/assorted/fixed_string.hpp"

namespace foedus {
namespace assorted {

DEFINE_TEST_CASE_PACKAGE(FixedStringTest, foedus.assorted);
TEST(FixedStringTest, Construct) {
  FixedString<8> str1;
  FixedString<12> str2;
  FixedString<34> str3;
  EXPECT_EQ(0, str1.length());
  EXPECT_EQ(0, str2.length());
  EXPECT_EQ(0, str3.length());
  EXPECT_TRUE(str1.empty());
  EXPECT_TRUE(str2.empty());
  EXPECT_TRUE(str3.empty());
}

TEST(FixedStringTest, Assign) {
  FixedString<8> str1;
  str1 = "12345";
  EXPECT_EQ(std::string("12345"), str1.str());
  EXPECT_EQ(5, str1.length());
  str1 = "aabc";
  EXPECT_EQ(std::string("aabc"), str1.str());
  EXPECT_EQ(4, str1.length());
}


TEST(FixedStringTest, AssignTruncate) {
  FixedString<8> str1;
  str1 = "0123456789";
  EXPECT_EQ(std::string("01234567"), str1.str());
  EXPECT_EQ(8, str1.length());
  str1 = "aabc";
  EXPECT_EQ(std::string("aabc"), str1.str());
  EXPECT_EQ(4, str1.length());
}

TEST(FixedStringTest, Copy) {
  FixedString<8> str1("12345");
  FixedString<12> str2;
  str2 = str1;
  EXPECT_EQ(std::string("12345"), str2.str());
  EXPECT_EQ(5, str2.length());
  FixedString<4> str3;
  str3 = str1;
  EXPECT_EQ(std::string("1234"), str3.str());
  EXPECT_EQ(4, str3.length());
}

TEST(FixedStringTest, Compare) {
  FixedString<8> str1("12345");
  FixedString<12> str2("123456");
  FixedString<16> str3("12345");
  EXPECT_EQ(str1, str3);
  EXPECT_NE(str2, str3);
  EXPECT_NE(str1, str2);
  EXPECT_LT(str1, str2);
  EXPECT_LT(str3, str2);
}

}  // namespace assorted
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(FixedStringTest, foedus.assorted);
