/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_MEMORY_ID_HPP_
#define FOEDUS_MEMORY_MEMORY_ID_HPP_
#include <numa.h>
#include <stdint.h>
/**
 * @file foedus/memory/memory_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup MEMORY
 */
namespace foedus {
namespace memory {

/**
 * @brief Offset in PagePool that compactly represents the page address (unlike 8 bytes pointer).
 * @ingroup MEMORY
 * @details
 * Offset 0 means nullptr. Page-0 never appears as a valid page.
 * In our engine, all page pools are per-NUMA node.
 * Thus, each NUMA node has its own page pool.
 * The maximum size of a single page pool is 2^32 * page size (4kb page: 16TB).
 * This should be sufficient for one NUMA node.
 */
typedef uint32_t PagePoolOffset;

/**
 * So far 2MB is the only page size available via Transparent Huge Page (THP).
 * @ingroup MEMORY
 */
const uint64_t HUGEPAGE_SIZE = 1 << 21;

/**
 * @brief Automatically sets and resets ::numa_set_preferred().
 * @ingroup MEMORY MEMHIERARCHY
 * @details
 * Use this in a place you want to direct all memory allocation to a specific NUMA node.
 */
struct ScopedNumaPreferred {
    explicit ScopedNumaPreferred(int numa_node) {
        ::numa_set_preferred(numa_node);
    }
    ~ScopedNumaPreferred() {
        ::numa_set_preferred(-1);
    }
};

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_MEMORY_ID_HPP_
