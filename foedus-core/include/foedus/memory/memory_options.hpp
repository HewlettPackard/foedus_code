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
#ifndef FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
#define FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
#include <stdint.h>

#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"

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
    /** Default value for page_pool_size_mb_per_node_. */
    kDefaultPagePoolSizeMbPerNode = 1 << 10,
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
   * @brief Whether to use non-transparent hugepages for big memories (1GB huge pages).
   * @details
   * To use this, you have to set up \e non-transparent hugepages that requires a reboot.
   * See the readme fore more details.
   */
  bool        use_mmap_hugepages_;

  /**
   * @brief Whether to use mprotect() for memory boundaries to detect bogus memory accesses.
   * @details
   * When true, we put a dummy mprotect-ed pages between chunks of shared memories.
   * Using this might cause performance degradation, but it helps debugging.
   * Default is false.
   */
  bool        rigorous_memory_boundary_check_;

  /**
   * @brief Whether to use mprotect() for page boundaries to detect bogus memory accesses.
   * @details
   * Similar to rigorous_memory_boundary_check_, but this one is about page pools.
   * When true, we \e waste a half of volatile/snapshot page pool, putting mprotect in odd-numbered
   * page indexes. The threads receive only the even-numbered indexes, so if any read/write
   * overruns a page, a signal is fired. This makes the execution quite slower and wastes
   * half of memory, so use it just for debugging. This flag also disables hugepages
   * used for snapshot page pool for mprotect to work, so makes it really SLOW.
   * Default is false.
   * @attention To use this feature with large page pool sizes, you must increase max_map_count.
   *   sudo sysctl -w vm.max_map_count=2147483647
   * Otherwise you will get out-of-memory crashes. Linux's default value is only 65530.
   * @note When FOEDUS is running on a valgrind process, you shouldn't turn this on because
   * valgrind uses a hard-coded limit VG_N_SEGMENTS rather than vm.max_map_count.
   * However, if you are sure the number of mprotect-ed pages are less than 64k, you can.
   * @see foedus::assorted::ProtectedBoundary
   */
  bool        rigorous_page_boundary_check_;

  /**
   * @brief Size of the page pool in MB per each NUMA node.
   * @details
   * Must be multiply of 2MB. Default is 1GB.
   * The total amount of memory is page_pool_size_mb_per_node_ *
   */
  uint32_t    page_pool_size_mb_per_node_;

  /**
   * @brief How many pages each NumaCoreMemory initially grabs when it is initialized.
   * @details
   * Default is 50% of PagePoolOffsetChunk::MAX_SIZE.
   * Obviously, private_page_pool_initial_grab_ * kPageSize * number-of-threads-per-node must be
   * within page_pool_size_mb_per_node_ to start up the engine.
   */
  uint32_t    private_page_pool_initial_grab_;

  EXTERNALIZABLE(MemoryOptions);
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_MEMORY_OPTIONS_HPP_
