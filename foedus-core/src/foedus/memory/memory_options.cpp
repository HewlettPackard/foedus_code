/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/memory_options.hpp>
#include <ostream>
namespace foedus {
namespace memory {
MemoryOptions::MemoryOptions() : use_numa_alloc_(true), interleave_numa_alloc_(false) {
}

std::ostream& operator<<(std::ostream& o, const MemoryOptions& v) {
    o << "Memory options:" << std::endl;
    o << "  use_numa_alloc_=" << v.use_numa_alloc_ << std::endl;
    o << "  interleave_numa_alloc_=" << v.interleave_numa_alloc_ << std::endl;
    return o;
}
}  // namespace memory
}  // namespace foedus
