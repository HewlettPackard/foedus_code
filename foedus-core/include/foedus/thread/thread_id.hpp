/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_ID_HPP_
#define FOEDUS_THREAD_THREAD_ID_HPP_
#include <foedus/cxx11.hpp>
#include <cstdint>
/**
 * @file thread_id.hpp
 * @brief Typedefs of ID types used in thread package.
 * @ingroup THREAD
 */
namespace foedus {
namespace thread {
/**
 * @typedef thread_group_id
 * @brief Typedef for an ID of ThreadGroup (NUMA node).
 * @ingroup THREAD
 * @details
 * Currently, we assume there are at most 256 NUMA nodes.
 */
typedef uint8_t thread_group_id;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
CXX11_STATIC_ASSERT (sizeof(thread_group_id) == 1, "Max NUMA node count must be 1 byte");
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

/**
 * @var MAX_THREAD_GROUP_ID
 * @brief Maximum possible value of thread_group_id.
 * @ingroup THREAD
 */
const thread_group_id MAX_THREAD_GROUP_ID = 0xFF;

/**
 * @typedef thread_local_ordinal
 * @brief Typedef for a \b local ID of Thread (core), which is \b NOT unique across NUMA nodes.
 * @ingroup THREAD
 * @details
 * Currently, we assume there are at most 256 cores per NUMA node.
 */
typedef uint8_t thread_local_ordinal;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
CXX11_STATIC_ASSERT (sizeof(thread_local_ordinal) == 1, "Max core-per-node must be 1 byte");
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

/**
 * @var MAX_THREAD_LOCAL_ORDINAL
 * @brief Maximum possible value of thread_local_ordinal.
 * @ingroup THREAD
 */
const thread_local_ordinal MAX_THREAD_LOCAL_ORDINAL = 0xFF;

/**
 * @typedef thread_id
 * @brief Typedef for a \b global ID of Thread (core), which is unique across NUMA nodes.
 * @ingroup THREAD
 * @details
 * This is a composite of thread_group_id (high 1 byte) and thread_local_ordinal (low 1 byte).
 * For example, if there are 2 NUMA nodes and 8 cores each:
 * thread-0=node-0 core-0, thread-1=node-0 core-1, ..., thread-256=node-1 core-0,...
 */
typedef uint16_t thread_id;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
CXX11_STATIC_ASSERT (sizeof(thread_id) == 2, "Max thread count must be 2 bytes");
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

/**
 * @var MAX_THREAD_ID
 * @brief Maximum possible value of thread_id.
 * @ingroup THREAD
 */
const thread_id MAX_THREAD_ID = 0xFFFF;

/**
 * Returns a globally unique ID of Thread (core) for the given node and ordinal in the node.
 * @ingroup THREAD
 */
inline thread_id compose_thread_id(thread_group_id node, thread_local_ordinal local_core) {
    return (node << 8) | local_core;
}

/**
 * Extracts NUMA node ID from the given globally unique ID of Thread (core).
 * @ingroup THREAD
 */
inline thread_id decompose_numa_node(thread_id global_id) {
    return (global_id >> 8) & 0xFF;
}

/**
 * Extracts local ordinal from the given globally unique ID of Thread (core).
 * @ingroup THREAD
 */
inline thread_id decompose_numa_local_ordinal(thread_id global_id) {
    return global_id & 0xFF;
}

}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_ID_HPP_
