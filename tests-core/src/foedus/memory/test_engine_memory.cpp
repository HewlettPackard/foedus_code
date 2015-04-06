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

#include <set>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/xct/xct_access.hpp"

namespace foedus {
namespace memory {
DEFINE_TEST_CASE_PACKAGE(EngineMemoryTest, foedus.memory);
TEST(EngineMemoryTest, SingleNode) {
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 2;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    EngineMemory* memory = engine.get_memory_manager();
    EXPECT_TRUE(memory->get_local_memory() == nullptr);
    EXPECT_TRUE(memory->get_node_memory(0) != nullptr);
    NumaNodeMemoryRef* ref = memory->get_node_memory(0);
    EXPECT_EQ(0, ref->get_numa_node());
    PagePool* volatile_pool = ref->get_volatile_pool();
    EXPECT_EQ(1U << 21, volatile_pool->get_memory_size());

    const GlobalVolatilePageResolver& resolver = memory->get_global_volatile_page_resolver();
    EXPECT_EQ(1U, resolver.numa_node_count_);
    EXPECT_EQ(volatile_pool->get_base(), resolver.bases_[0]);
    EXPECT_GT(resolver.begin_, 0);
    EXPECT_LT(resolver.begin_, resolver.end_);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(EngineMemoryTest, TwoNodes) {
  if (!is_multi_nodes()) {
    return;
  }
  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = 2;
  options.memory_.page_pool_size_mb_per_node_ = 2;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    EngineMemory* memory = engine.get_memory_manager();
    EXPECT_TRUE(memory->get_local_memory() == nullptr);
    EXPECT_TRUE(memory->get_node_memory(0) != nullptr);
    EXPECT_TRUE(memory->get_node_memory(1) != nullptr);
    NumaNodeMemoryRef* ref[2];
    for (uint16_t i = 0; i < 2U; ++i) {
      ref[i] = memory->get_node_memory(i);
      EXPECT_EQ(i, ref[i]->get_numa_node());
      PagePool* volatile_pool = ref[i]->get_volatile_pool();
      EXPECT_EQ(1U << 21, volatile_pool->get_memory_size());
    }

    const GlobalVolatilePageResolver& resolver = memory->get_global_volatile_page_resolver();
    EXPECT_EQ(2U, resolver.numa_node_count_);
    for (uint16_t i = 0; i < 2U; ++i) {
      EXPECT_EQ(ref[i]->get_volatile_pool()->get_base(), resolver.bases_[i]);
    }
    EXPECT_GT(resolver.begin_, 0);
    EXPECT_LT(resolver.begin_, resolver.end_);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace memory
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(EngineMemoryTest, foedus.memory);
