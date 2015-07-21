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
#include "foedus/memory/page_pool.hpp"
#include "foedus/memory/page_pool_pimpl.hpp"

namespace foedus {
namespace memory {
DEFINE_TEST_CASE_PACKAGE(PagePoolTest, foedus.memory);

void verify_full_pool(PagePool* pool) {
  const uint32_t free_pool_pages = pool->get_resolver().begin_;
  const uint64_t pool_size = (pool->get_memory_size() / (1U << 12)) - free_pool_pages;
  uint16_t count[sizeof(PagePoolOffsetChunk) / sizeof(PagePoolOffset)];
  std::memset(count, 0, sizeof(count));
  PagePoolOffsetChunk chunk;
  EXPECT_EQ(kErrorCodeOk, pool->grab(PagePoolOffsetChunk::kMaxSize, &chunk));
  EXPECT_EQ(pool_size, chunk.size());
  PagePoolOffsetChunk chunk2;
  while (!chunk.empty()) {
    PagePoolOffset offset = chunk.pop_back();
    EXPECT_GE(offset, free_pool_pages);
    EXPECT_LT(offset, sizeof(PagePoolOffsetChunk) / sizeof(PagePoolOffset));
    EXPECT_EQ(0, count[offset]);
    ++count[offset];
    chunk2.push_back(offset);
  }
  pool->release(chunk2.size(), &chunk2);
}

TEST(PagePoolTest, Construct) {
  const uint64_t kPoolSize = 1ULL << 21;
  AlignedMemory block_memory;
  block_memory.alloc(1U << 12, 1U << 12, AlignedMemory::kNumaAllocOnnode, 0);
  EXPECT_TRUE(block_memory.get_block() != nullptr);
  PagePoolControlBlock* block = reinterpret_cast<PagePoolControlBlock*>(block_memory.get_block());
  AlignedMemory pool_memory;
  pool_memory.alloc(kPoolSize, 1U << 21, AlignedMemory::kNumaAllocOnnode, 0);
  EXPECT_TRUE(pool_memory.get_block() != nullptr);

  PagePool pool;
  pool.attach(block, pool_memory.get_block(), kPoolSize, true);
  COERCE_ERROR(pool.initialize());
  EXPECT_EQ(kPoolSize, pool.get_memory_size());

  verify_full_pool(&pool);

  PagePool pool_ref;
  pool_ref.attach(block, pool_memory.get_block(), kPoolSize, false);
  COERCE_ERROR(pool_ref.initialize());
  EXPECT_EQ(kPoolSize, pool_ref.get_memory_size());
  COERCE_ERROR(pool_ref.uninitialize());

  verify_full_pool(&pool);

  COERCE_ERROR(pool.uninitialize());
}


TEST(PagePoolTest, GrabRelease) {
  // pool size less than one full PagePoolOffsetChunk (note: a few pages spent for free-pool pages)
  const uint64_t kPoolSize = (1ULL << 12) * sizeof(PagePoolOffsetChunk) / sizeof(PagePoolOffset);
  AlignedMemory block_memory;
  block_memory.alloc(1U << 12, 1U << 12, AlignedMemory::kNumaAllocOnnode, 0);
  PagePoolControlBlock* block = reinterpret_cast<PagePoolControlBlock*>(block_memory.get_block());
  AlignedMemory pool_memory;
  pool_memory.alloc(kPoolSize, 1U << 21, AlignedMemory::kNumaAllocOnnode, 0);

  PagePool pool;
  pool.attach(block, pool_memory.get_block(), kPoolSize, true);
  COERCE_ERROR(pool.initialize());
  EXPECT_EQ(kPoolSize, pool.get_memory_size());

  PagePoolOffsetChunk chunk;
  EXPECT_EQ(0, chunk.size());
  EXPECT_FALSE(chunk.full());
  EXPECT_EQ(kErrorCodeOk, pool.grab(PagePoolOffsetChunk::kMaxSize, &chunk));
  const uint32_t free_pool_pages = pool.get_resolver().begin_;
  EXPECT_GT(free_pool_pages, 0);
  EXPECT_EQ((kPoolSize /  (1ULL << 12)) - free_pool_pages, chunk.size());
  EXPECT_FALSE(chunk.full());

  PagePoolOffsetChunk chunk2;
  EXPECT_EQ(kErrorCodeMemoryNoFreePages, pool.grab(PagePoolOffsetChunk::kMaxSize, &chunk2));
  EXPECT_EQ(0, chunk2.size());

  uint32_t size_before = chunk.size();
  pool.release(chunk.size() / 2U, &chunk);
  EXPECT_EQ(size_before - (size_before / 2U), chunk.size());

  EXPECT_EQ(kErrorCodeOk, pool.grab(PagePoolOffsetChunk::kMaxSize, &chunk2));
  EXPECT_EQ(size_before / 2U, chunk2.size());
  pool.release(chunk2.size(), &chunk2);
  EXPECT_EQ(0, chunk2.size());

  pool.release(chunk.size(), &chunk);
  EXPECT_EQ(0, chunk.size());

  verify_full_pool(&pool);

  COERCE_ERROR(pool.uninitialize());
}

TEST(PagePoolTest, GrabReleaseWithEpoch) {
  const uint64_t kPoolSize = (1ULL << 12) * sizeof(PagePoolOffsetChunk) / sizeof(PagePoolOffset);
  const uint64_t kPoolSizeInPages = (kPoolSize /  (1ULL << 12));
  AlignedMemory block_memory;
  block_memory.alloc(1U << 12, 1U << 12, AlignedMemory::kNumaAllocOnnode, 0);
  PagePoolControlBlock* block = reinterpret_cast<PagePoolControlBlock*>(block_memory.get_block());
  AlignedMemory pool_memory;
  pool_memory.alloc(kPoolSize, 1U << 21, AlignedMemory::kNumaAllocOnnode, 0);

  PagePool pool;
  pool.attach(block, pool_memory.get_block(), kPoolSize, true);
  COERCE_ERROR(pool.initialize());
  EXPECT_EQ(kPoolSize, pool.get_memory_size());

  PagePoolOffsetChunk chunk;
  EXPECT_EQ(kErrorCodeOk, pool.grab(PagePoolOffsetChunk::kMaxSize, &chunk));
  const uint32_t free_pool_pages = pool.get_resolver().begin_;

  PagePoolOffsetAndEpochChunk epoch_chunk;
  for (uint32_t i = free_pool_pages; i < kPoolSizeInPages; ++i) {
    epoch_chunk.push_back(i, Epoch(i / 3U));
  }
  EXPECT_EQ(kPoolSizeInPages - free_pool_pages, epoch_chunk.size());

  uint32_t half_count = epoch_chunk.get_safe_offset_count(Epoch(kPoolSizeInPages / 6U));
  EXPECT_EQ(kPoolSizeInPages / 6U, (half_count + free_pool_pages) / 3U);
  EXPECT_EQ((kPoolSizeInPages / 6U) - 1U, (half_count + free_pool_pages - 1U) / 3U);

  pool.release(half_count, &epoch_chunk);
  EXPECT_EQ(kPoolSizeInPages - free_pool_pages - half_count, epoch_chunk.size());

  pool.release(epoch_chunk.size(), &epoch_chunk);

  verify_full_pool(&pool);

  COERCE_ERROR(pool.uninitialize());
}

}  // namespace memory
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(PagePoolTest, foedus.memory);
