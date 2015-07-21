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
