/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
#include "foedus/xct/xct_access.hpp"

namespace foedus {
namespace memory {
DEFINE_TEST_CASE_PACKAGE(EngineMemoryTest, foedus.memory);
TEST(EngineMemoryTest, Empty) {
}
}  // namespace memory
}  // namespace foedus
