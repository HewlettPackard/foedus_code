/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_MEMORY_HIERARCHY_HPP_
#define FOEDUS_MEMORY_MEMORY_HIERARCHY_HPP_

/**
 * @defgroup MEMHIERARCHY Memory Hierarchy
 * @brief NUMA-Aware Memory Hierarchy in libfoedus-core
 * @ingroup MEMORY
 * @details
 * @par Overview
 * We have a memory hierarchy of three levels in libfoedus, resembling the memory hiearchy in
 * NUMA architecture:
 *   \li Memories shared engine-wide (EngineMemory)
 *   \li Memories shared NUMA-Node-wide (NumaNodeMemory)
 *   \li Private Memories in each core (NumaCoreMemory)
 *
 * The resemblance is intentinal to achieve the best performance of memory
 * allocation/deallocation/sharing in NUMA setting.
 *
 * @par Absolutely no global nor truly TLS variables
 * You might notice that the top level of the hierarchy is \e engine-wide, not \e global.
 * libfoedus uses absolutely no global nor static variables except const primitive types.
 * This simplifies the design of memory management in the library and allows
 * running multiple instances (engines) of our library even in one process.
 * What we have in Engine object is everything. When Engine's uninitialize() is invoked,
 * everything the Engine acquired is released, separately from other Engine's.
 * This also means that memory-leak checkers like valgrind can easily check for potential errors.
 *
 * @par Memories shared engine-wide (EngineMemory)
 * bluh
 * @par Memories shared NUMA-Node-wide (NumaNodeMemory)
 * bluh
 * @par Private Memories in each core (NumaCoreMemory)
 * bluh
 */

#endif  // FOEDUS_MEMORY_MEMORY_HIERARCHY_HPP_
