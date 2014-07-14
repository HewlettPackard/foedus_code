/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_NAMESPACE_INFO_HPP_
#define FOEDUS_CACHE_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::cache
 * @brief \b Snapshot \b Cache \b Manager, which caches data pages retrieved from snapshot files.
 * @details
 * @section CACHE_OVERVIEW Overview
 * Snapshot Cache is a node-local read-only cache of snapshot pages.
 * In some sense, it is a special kind of bufferpool that is much simpler and faster than
 * traditional implementations due to the special requirements; no modifications, no interaction
 * with logging module nor commit protocol, no guarantee needed for uniqueness (no problem to
 * occasionally have two cached instances of the same page; just a bit waste of space).
 * The operation to retrieve a snapshot page, especially when the page is in this cache,
 * must be VERY fast. Thus, this cache manager must be highly optimized for read operations.
 *
 * @section CACHE_STRUCTURE Structure
 * For each NUMA node memory, we have a hashtable structure whose key is snapshot-id/page-id and
 * whose value is offset in cache memory.
 */

/**
 * @defgroup CACHE Snapshot Cache Manager
 * @ingroup COMPONENTS
 * @copydoc foedus::cache
 */

#endif  // FOEDUS_CACHE_NAMESPACE_INFO_HPP_
