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
#ifndef FOEDUS_MEMORY_MEMORY_ID_HPP_
#define FOEDUS_MEMORY_MEMORY_ID_HPP_

#include <numa.h>
#include <stdint.h>

#include "foedus/assorted/mod_numa_node.hpp"

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
const uint64_t kHugepageSize = 1 << 21;

/**
 * @brief Automatically sets and resets ::numa_set_preferred().
 * @ingroup MEMORY MEMHIERARCHY
 * @details
 * Use this in a place you want to direct all memory allocation to a specific NUMA node.
 */
struct ScopedNumaPreferred {
  ScopedNumaPreferred(int numa_node, bool retain_old = false) {
    // if the machine is not a NUMA machine (1-socket), then avoid calling libnuma functions.
    numa_enabled_ = (::numa_available() >= 0);
    if (!numa_enabled_) {
      numa_node = 0;
      return;
    }
    if (retain_old) {
      old_value_ = ::numa_preferred();
    } else {
      old_value_ = -1;
    }
    // in order to run even on a non-numa machine or a machine with fewer sockets,
    // we allow specifying arbitrary numa_node. we just take rem.
    numa_node = assorted::mod_numa_node(numa_node);
    ::numa_set_preferred(numa_node);
  }
  ~ScopedNumaPreferred() {
    if (numa_enabled_) {
      ::numa_set_preferred(old_value_);
    }
  }
  int old_value_;
  bool numa_enabled_;
};

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_MEMORY_ID_HPP_
