/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
#define FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <stdint.h>
namespace foedus {
namespace memory {
/**
 * @brief Set of options for memory manager.
 * @ingroup MEMORY
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct MemoryOptions CXX11_FINAL : public virtual externalize::Externalizable {
    /** Constant values. */
    enum Constants {
        /** Default value for page_pool_size_mb_. */
        DEFAULT_PAGE_POOL_SIZE_MB = 1 << 10,
    };

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
     * @brief Total size of the page pool in MB.
     * @details
     * Default is 1GB.
     */
    uint32_t    page_pool_size_mb_;

    /**
     * @brief How many pages each NumaCoreMemory initially grabs when it is initialized.
     * @details
     * Default is 50% of PagePoolOffsetChunk::MAX_SIZE.
     * Obviously, private_page_pool_initial_grab_ * PAGE_SIZE * number-of-threads must be
     * within page_pool_size_mb_ to start up the engine.
     */
    uint32_t    private_page_pool_initial_grab_;

    EXTERNALIZABLE(MemoryOptions);
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
