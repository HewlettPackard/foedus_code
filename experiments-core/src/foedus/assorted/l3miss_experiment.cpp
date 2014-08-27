/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */

// Tests the cost of L3 cache miss.
#include <iostream>

#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/thread/numa_thread_scope.hpp"

const uint64_t kMemory = 1ULL << 33;
const uint32_t kRands = 1 << 24;
const uint32_t kRep = 1ULL << 30;

uint64_t run(const char* blocks, const uint32_t* rands) {
  uint64_t ret = 0;
  for (uint32_t i = 0; i < kRep; ++i) {
    const char* block = blocks + ((rands[i % kRands] % (kMemory >> 6)) << 6);
    ret += *block;
  }
  return ret;
}

int main(int /*argc*/, char **/*argv*/) {
  foedus::thread::NumaThreadScope scope(0);
  foedus::memory::AlignedMemory memory;
  memory.alloc(kMemory, 1ULL << 30, foedus::memory::AlignedMemory::kNumaAllocOnnode, 0);

  foedus::memory::AlignedMemory rand_memory;
  rand_memory.alloc(kRands * 4, 1 << 21, foedus::memory::AlignedMemory::kNumaAllocOnnode, 0);

  foedus::assorted::UniformRandom uniform_random(1234);
  uniform_random.fill_memory(&rand_memory);
  uint32_t* rands = reinterpret_cast<uint32_t*>(rand_memory.get_block());

  {
    foedus::debugging::StopWatch stop_watch;
    uint64_t ret = run(reinterpret_cast<char*>(memory.get_block()), rands);
    stop_watch.stop();
    std::cout << "run(ret=" << ret << ") in " << stop_watch.elapsed_ms() << " ms" << std::endl;
    std::cout << "On average, " << (static_cast<double>(stop_watch.elapsed_ns()) / kRep)
      << " ns/miss" << std::endl;
  }
  return 0;
}
