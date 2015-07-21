/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
