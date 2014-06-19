/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/uniform_random.hpp>
#include <foedus/memory/aligned_memory.hpp>
namespace foedus {
namespace assorted {

void UniformRandom::fill_memory(memory::AlignedMemory* memory) {
  uint32_t* ints = reinterpret_cast<uint32_t*>(memory->get_block());
  uint64_t count = memory->get_size() / sizeof(uint32_t);
  for (uint64_t i = 0; i < count; ++i) {
    ints[i] = next_uint32();
  }
}

}  // namespace assorted
}  // namespace foedus

