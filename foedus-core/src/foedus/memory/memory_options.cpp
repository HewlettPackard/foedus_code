/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/memory_options.hpp>
#include <ostream>
namespace foedus {
namespace memory {
MemoryOptions::MemoryOptions() {
}
}  // namespace memory
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::memory::MemoryOptions& v) {
    o << "Memory options:" << std::endl;
    return o;
}
