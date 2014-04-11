/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
#define FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
#include <stdint.h>
#include <iosfwd>
namespace foedus {
namespace memory {
/**
 * @brief Set of options for memory manager.
 * @ingroup MEMORY
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct MemoryOptions {
    /**
     * Constructs option values with default values.
     */
    MemoryOptions();

    /**
     * @brief Whether to use ::numa_alloc_interleaved()/::numa_alloc_onnode() to allocate memories
     * in NumaCoreMemory and NumaNodeMemory.
     * @details
     * If false, we use usual posix_memalign() instead.
     * If everything works correctly, ::numa_alloc_interleaved()/::numa_alloc_onnode()
     * should result in much better performance
     * because each thread should access only the memories allocated for the NUMA node.
     * Default is true.
     */
    bool        use_numa_alloc_;

    /**
     * @brief Whether to use ::numa_alloc_interleaved() instead of ::numa_alloc_onnode().
     * @details
     * If everything works correctly, numa_alloc_onnode() should result in much better performance
     * because interleaving just wastes memory if it is very rare to access other node's memory.
     * Default is false.
     * If use_numa_alloc_ is false, this configuration has no meaning.
     */
    bool        interleave_numa_alloc_;

    /**
     * @brief Total size of the page pool for volatile pages in MB.
     * @details
     *
     */
    uint32_t    volatile_page_pool_size_mb_;

    friend std::ostream& operator<<(std::ostream& o, const MemoryOptions& v);
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
