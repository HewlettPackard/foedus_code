/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */

// Just a few perf tests around std::sort.
#include <algorithm>
#include <iostream>

#include "foedus/compiler.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/memory_id.hpp"

const uint32_t kRep = 20;
const uint64_t kEntries = 1ULL << 20;

struct Separate {
  uint64_t key_;
  uint64_t data_;
  inline bool operator<(const Separate& rhs) const ALWAYS_INLINE {
    return key_ < rhs.key_;
  }
};

struct Both {
  __uint128_t both_;
};

double run_separate(bool block_sorted, Separate* buf) {
  double total = 0;
  for (uint32_t rep = 0; rep < kRep; ++rep) {
    if (block_sorted) {
      for (uint32_t i = 0; i < kEntries; ++i) {
        buf[i].key_ = i % (kEntries >> 3);
        buf[i].data_ = 0;
      }
    } else {
      foedus::assorted::UniformRandom uniform_random(1234);
      for (uint32_t i = 0; i < kEntries; ++i) {
        buf[i].key_ = uniform_random.next_uint64();
        buf[i].data_ = 0;
      }
    }
    foedus::debugging::StopWatch stop_watch;
    std::sort(buf, buf + kEntries);
    stop_watch.stop();
    total += stop_watch.elapsed_ms();
  }
  return total / kRep;
}

double run_both(bool block_sorted, Both* buf) {
  double total = 0;
  for (uint32_t rep = 0; rep < kRep; ++rep) {
    if (block_sorted) {
      for (uint32_t i = 0; i < kEntries; ++i) {
        buf[i].both_ = static_cast<__uint128_t>(i % (kEntries >> 3)) << 64;
      }
    } else {
      foedus::assorted::UniformRandom uniform_random(1234);
      for (uint32_t i = 0; i < kEntries; ++i) {
        buf[i].both_ = static_cast<__uint128_t>(uniform_random.next_uint64()) << 64;
      }
    }
    foedus::debugging::StopWatch stop_watch;
    std::sort(&(buf->both_), &(buf[kEntries].both_));
    stop_watch.stop();
    total += stop_watch.elapsed_ms();
  }
  return total / kRep;
}

int main(int /*argc*/, char **/*argv*/) {
  foedus::memory::ScopedNumaPreferred scope(0);
  foedus::memory::AlignedMemory memory;
  memory.alloc(kEntries * 16ULL, 1 << 21, foedus::memory::AlignedMemory::kNumaAllocOnnode, 0);

  void* buf = memory.get_block();
  std::cout << "separate_rand: "
    << run_separate(false, reinterpret_cast<Separate*>(buf)) << " ms" << std::endl;
  std::cout << "separate_block: "
    << run_separate(true, reinterpret_cast<Separate*>(buf)) << " ms" << std::endl;
  std::cout << "both_rand: "
    << run_both(false, reinterpret_cast<Both*>(buf)) << " ms" << std::endl;
  std::cout << "both_block: "
    << run_both(true, reinterpret_cast<Both*>(buf)) << " ms" << std::endl;
  return 0;
}
// on Z820
// separate_rand: 72.8076 ms
// separate_block: 34.5506 ms
// both_rand: 63.4029 ms
// both_block: 41.8907 ms
// Conclusion. for really random input, uint128_t for both would make sense. for merge-sort, no.


