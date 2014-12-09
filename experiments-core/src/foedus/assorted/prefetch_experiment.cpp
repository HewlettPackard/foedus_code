/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */

// Just testing an expected thing.
#include <iostream>

#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/memory_id.hpp"

const uint32_t kRands = 1 << 10;
const uint32_t kRep = 1 << 24;

uint64_t run(bool prefetch, const char* blocks, const uint32_t* rands) {
  uint64_t ret = 0;
  for (uint32_t i = 0; i < kRep; ++i) {
    // const char* block = blocks + ((rands[i % kRands] & 0xFFF) << 12);
    const char* block = blocks + ((rands[i % kRands] & 0xFFF) << 12);
    // 813ms vs 238ms. expected.
    if (prefetch) {
      foedus::assorted::prefetch_cacheline(block + 0x180);
      foedus::assorted::prefetch_cacheline(block + 0x3C0);
      foedus::assorted::prefetch_cacheline(block + 0x840);
      foedus::assorted::prefetch_cacheline(block + 0xC80);
      /*
      const ::_mm_hint hint = ::_MM_HINT_T0;
      // ::_mm_hint hint = ::_MM_HINT_NTA;  // this one has much less difference.
      ::_mm_prefetch(block + 0x180, hint);
      ::_mm_prefetch(block + 0x3C0, hint);
      ::_mm_prefetch(block + 0x840, hint);
      ::_mm_prefetch(block + 0xC80, hint);
      */
      // the following makes it 2x slower, thus prefetch size is actually 64 bytes
      // ::_mm_prefetch(block + 0x180, hint);
      // ::_mm_prefetch(block + 0x380, hint);
      // ::_mm_prefetch(block + 0x800, hint);
      // ::_mm_prefetch(block + 0xC80, hint);
      // but this says it's 128 bytes:
      // https://software.intel.com/en-us/articles/use-software-data-prefetch-on-32-bit-intel-architecture
      // wtf?
      // -> ah, I see. the behavior is different because it's 64bit intel.
    }
    ret += block[421 + ret];
    ret += block[1000 + ret];
    ret += block[2143 + ret];
    ret += block[3245 + ret];
  }
  return ret;
}

int main(int /*argc*/, char **/*argv*/) {
  foedus::memory::ScopedNumaPreferred scope(0);
  foedus::memory::AlignedMemory memory;
  memory.alloc(1 << 24, 1 << 21, foedus::memory::AlignedMemory::kNumaAllocOnnode, 0);

  foedus::memory::AlignedMemory rand_memory;
  rand_memory.alloc(1 << 21, 1 << 21, foedus::memory::AlignedMemory::kNumaAllocOnnode, 0);

  foedus::assorted::UniformRandom uniform_random(1234);
  uint32_t* rands = reinterpret_cast<uint32_t*>(rand_memory.get_block());
  for (uint32_t i = 0; i < kRands; ++i) {
    rands[i] = uniform_random.next_uint32();
  }

  {
    foedus::debugging::StopWatch stop_watch;
    uint64_t ret = run(false, reinterpret_cast<char*>(memory.get_block()), rands);
    stop_watch.stop();
    std::cout << "run(ret=" << ret << ") in " << stop_watch.elapsed_ms() << " ms" << std::endl;
  }
  {
    foedus::debugging::StopWatch stop_watch;
    uint64_t ret = run(true, reinterpret_cast<char*>(memory.get_block()), rands);
    stop_watch.stop();
    std::cout << "pre(ret=" << ret << ") in " << stop_watch.elapsed_ms() << " ms" << std::endl;
  }
  return 0;
}
