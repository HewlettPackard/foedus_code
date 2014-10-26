/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <stdint.h>

#include "foedus/test_common.hpp"
#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace assorted {

DEFINE_TEST_CASE_PACKAGE(AssortedTest, foedus.assorted);

uint64_t align_4kb(uint64_t value) { return align< uint64_t, (1U << 12) >(value); }
uint64_t align_2mb(uint64_t value) { return align< uint64_t, (1U << 21) >(value); }

TEST(AssortedTest, Align32) {
  EXPECT_EQ(4U << 10, align_4kb((4U << 10)));
  EXPECT_EQ(8U << 10, align_4kb((4U << 10) + 42));
  EXPECT_EQ(8U << 10, align_4kb((8U << 10) - 1U));
  EXPECT_EQ(6ULL << 20, align_2mb((4ULL << 20) + 32));
  EXPECT_EQ(6ULL << 20, align_2mb((6ULL << 20) - 1ULL));
  EXPECT_EQ(4ULL << 20, align_2mb((4ULL << 20)));
}

TEST(AssortedTest, Align64) {
  const uint64_t kBigNumber = (1ULL << 32);
  EXPECT_EQ(kBigNumber + (4U << 10), align_4kb(kBigNumber + (4U << 10)));
  EXPECT_EQ(kBigNumber + (8U << 10), align_4kb(kBigNumber + (4U << 10) + 42));
  EXPECT_EQ(kBigNumber + (8U << 10), align_4kb(kBigNumber + (8U << 10) - 1U));
  EXPECT_EQ(kBigNumber + (6ULL << 20), align_2mb(kBigNumber + (4ULL << 20) + 32));
  EXPECT_EQ(kBigNumber + (6ULL << 20), align_2mb(kBigNumber + (6ULL << 20) - 1ULL));
  EXPECT_EQ(kBigNumber + (4ULL << 20), align_2mb(kBigNumber + (4ULL << 20)));
}

}  // namespace assorted
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(AssortedTest, foedus.assorted);
