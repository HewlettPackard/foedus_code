/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
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
    o << "  <MemoryOptions>" << std::endl;
    EXTERNALIZE_WRITE(use_numa_alloc_);
    EXTERNALIZE_WRITE(interleave_numa_alloc_);
    EXTERNALIZE_WRITE(page_pool_size_mb_);
    o << "  </MemoryOptions>" << std::endl;
    return o;
}
}  // namespace memory
}  // namespace foedus
