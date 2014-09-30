/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/memory/aligned_memory.hpp"

namespace foedus {
namespace debugging {
DEFINE_TEST_CASE_PACKAGE(DebuggingOptionsTest, foedus.debugging);

TEST(DebuggingOptionsTest, Profile) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    engine.get_debug()->start_papi_counters();
    /*
    memory::AlignedMemory memory;
    memory.alloc(1 << 24, 1 << 21, memory::AlignedMemory::kNumaAllocOnnode, 0);
    assorted::UniformRandom rnd(1234L);
    uint32_t a = 0;
    for (uint64_t i = 0; i < 1000; ++i) {
      uint32_t addr = rnd.next_uint32() % (1 << 24);
      a += *(reinterpret_cast<char*>(memory.get_block()) + addr);
    }
    std::cout << "a=" << a << std::endl;
    */
    engine.get_debug()->stop_papi_counters();
    DebuggingSupports::PapiCounters counters = engine.get_debug()->get_papi_counters();
    std::vector<std::string> results = DebuggingSupports::describe_papi_counters(counters);
    for (const std::string& result : results) {
      std::cout << result << std::endl;
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace debugging
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(DebuggingOptionsTest, foedus.debugging);
