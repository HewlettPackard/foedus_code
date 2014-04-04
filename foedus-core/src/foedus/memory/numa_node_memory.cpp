/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/numa_node_memory.hpp>
namespace foedus {
namespace memory {
NumaNodeMemory::NumaNodeMemory() : initialized_(false) {
}
NumaNodeMemory::~NumaNodeMemory() {
}
}  // namespace memory
}  // namespace foedus
