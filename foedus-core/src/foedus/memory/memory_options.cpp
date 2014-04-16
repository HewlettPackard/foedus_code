/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/memory_options.hpp>
#include <ostream>
namespace foedus {
namespace memory {
MemoryOptions::MemoryOptions() {
    use_numa_alloc_ = true;
    interleave_numa_alloc_ = false;
    page_pool_size_mb_ = DEFAULT_PAGE_POOL_SIZE_MB;
}

std::ostream& operator<<(std::ostream& o, const MemoryOptions& v) {
    o << "Memory options:" << std::endl;
    o << "  use_numa_alloc_=" << v.use_numa_alloc_ << std::endl;
    o << "  interleave_numa_alloc_=" << v.interleave_numa_alloc_ << std::endl;
    o << "  page_pool_size_mb_=" << v.page_pool_size_mb_ << std::endl;
    return o;
}
}  // namespace memory
}  // namespace foedus
