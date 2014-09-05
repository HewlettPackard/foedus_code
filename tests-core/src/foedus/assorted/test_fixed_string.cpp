/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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

}  // namespace assorted
}  // namespace foedus
