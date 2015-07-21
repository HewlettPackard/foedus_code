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

#include "foedus/test_common.hpp"
#include "foedus/memory/aligned_memory.hpp"

/**
 * @file test_aligned_memory.cpp
 * Testcases for AlignedMemory.
 * We should also run valgrind on this testcase especially for memory leak.
 */
namespace foedus {
namespace memory {

DEFINE_TEST_CASE_PACKAGE(AlignedMemoryTest, foedus.memory);

TEST(AlignedMemoryTest, Instantiate) {
  AlignedMemory memory;
  EXPECT_EQ(nullptr, memory.get_block());
  EXPECT_TRUE(memory.is_null());
  for (AlignedMemory::AllocType type = AlignedMemory::kPosixMemalign;
      type <= AlignedMemory::kNumaAllocOnnode;
      type = static_cast<AlignedMemory::AllocType>(static_cast<int>(type) + 1)) {
    AlignedMemory memory2(1 << 20, 1 << 13, type, 0);
    EXPECT_NE(nullptr, memory2.get_block());
    EXPECT_EQ(1 << 13, memory2.get_alignment());
    EXPECT_EQ(type, memory2.get_alloc_type());
    EXPECT_FALSE(memory2.is_null());
  }
}
TEST(AlignedMemoryTest, Instantiate2) {
  AlignedMemory memory;
  memory.release_block();
  EXPECT_EQ(nullptr, memory.get_block());
  EXPECT_TRUE(memory.is_null());
  for (AlignedMemory::AllocType type = AlignedMemory::kPosixMemalign;
      type <= AlignedMemory::kNumaAllocOnnode;
      type = static_cast<AlignedMemory::AllocType>(static_cast<int>(type) + 1)) {
    AlignedMemory memory2(1 << 20, 1 << 13, type, 0);
    memory2.release_block();
    EXPECT_EQ(nullptr, memory2.get_block());
    EXPECT_TRUE(memory.is_null());
  }
}

TEST(AlignedMemoryTest, Move) {
  AlignedMemory memory;
  for (AlignedMemory::AllocType type = AlignedMemory::kPosixMemalign;
      type <= AlignedMemory::kNumaAllocOnnode;
      type = static_cast<AlignedMemory::AllocType>(static_cast<int>(type) + 1)) {
    AlignedMemory memory2(1 << 20, 1 << 12, type, 0);
    memory = std::move(memory2);
  }
}

uint64_t pointer_distance(void* from, void* to) {
  return reinterpret_cast<char*>(to) - reinterpret_cast<char*>(from);
}

TEST(AlignedMemoryTest, Slice) {
  AlignedMemory memory(1 << 20, 1 << 13, AlignedMemory::kNumaAllocOnnode, 0);
  AlignedMemorySlice slice1(&memory);
  EXPECT_EQ(memory.get_block(), slice1.get_block());
  EXPECT_EQ(memory.get_size(), slice1.get_size());
  EXPECT_TRUE(slice1.is_valid());
  AlignedMemorySlice slice2 = slice1;
  AlignedMemorySlice slice3(slice2, 1 << 18, 3 << 18);
  AlignedMemorySlice slice4(slice3, 1 << 18, 1 << 18);

  EXPECT_EQ(0, pointer_distance(memory.get_block(), slice1.get_block()));
  EXPECT_EQ(1 << 20, slice1.get_size());

  EXPECT_EQ(0, pointer_distance(memory.get_block(), slice2.get_block()));
  EXPECT_EQ(1 << 20, slice2.get_size());

  EXPECT_EQ(1 << 18, pointer_distance(memory.get_block(), slice3.get_block()));
  EXPECT_EQ(3 << 18, slice3.get_size());

  EXPECT_EQ(2 << 18, pointer_distance(memory.get_block(), slice4.get_block()));
  EXPECT_EQ(1 << 18, slice4.get_size());
}
}  // namespace memory
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(AlignedMemoryTest, foedus.memory);
