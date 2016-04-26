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
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/spin_until_impl.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/memory/aligned_memory.hpp"

namespace foedus {
namespace debugging {
DEFINE_TEST_CASE_PACKAGE(DebuggingOptionsTest, foedus.debugging);

TEST(DebuggingOptionsTest, Profile) {
  if (assorted::is_running_on_valgrind()) {
    std::cout << "PAPI counters seem to have many valgrind issues." << std::endl;
    std::cout << "We only occasionally turn it on, so we skip this test in valgrind" << std::endl;
    return;
  }
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
