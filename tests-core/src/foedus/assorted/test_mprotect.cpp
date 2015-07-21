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

#include <string>

#include "foedus/test_common.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/protected_boundary.hpp"
#include "foedus/memory/aligned_memory.hpp"

namespace foedus {
namespace assorted {

DEFINE_TEST_CASE_PACKAGE(MprotectTest, foedus.assorted);

void test(uint64_t alignment) {
  memory::AlignedMemory m;
  const uint32_t kPages = 16;
  m.alloc(
    kPages * sizeof(ProtectedBoundary),
    alignment,
    memory::AlignedMemory::kNumaAllocOnnode,
    0);
  EXPECT_FALSE(m.is_null());

  ProtectedBoundary* pages = reinterpret_cast<ProtectedBoundary*>(m.get_block());

  // even if the pages are hugepages, mprotect can specify granular ranges.
  const uint32_t kCulprit = 12;
  for (uint32_t i = 0; i < kPages; ++i) {
    pages[i].reset("aaa");
    if (i == kCulprit) {
      pages[i].acquire_protect();
    }
  }

  uint32_t ret = 0;
  for (uint32_t i = 0; i < kPages; ++i) {
    if (i != kCulprit) {
      ret += pages[i].data_[0];
      ret += pages[i].data_[ProtectedBoundary::kWordCount - 1U];
      assorted::prefetch_cacheline(pages + i + 1);
    }
  }

  for (uint32_t i = 0; i < kPages; ++i) {
    if (i == kCulprit) {
      pages[i].release_protect();
    }
    EXPECT_EQ(std::string("aaa"), pages[i].get_boundary_name());
    for (uint32_t j = 0; j < ProtectedBoundary::kWordCount; ++j) {
      EXPECT_EQ(kProtectedBoundaryMagicWord, pages[i].data_[j]) << i << "," << j;
    }
    pages[i].assert_boundary();
  }
  m.release_block();
}

TEST(MprotectTest, SmallPage) { test(1U << 12); }
TEST(MprotectTest, HugePage)  { test(1U << 21); }

}  // namespace assorted
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MprotectTest, foedus.assorted);
