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
#include "foedus/memory/memory_options.hpp"

#include "foedus/externalize/externalizable.hpp"
#include "foedus/memory/page_pool.hpp"

namespace foedus {
namespace memory {
MemoryOptions::MemoryOptions() {
  use_numa_alloc_ = true;
  interleave_numa_alloc_ = false;
  use_mmap_hugepages_ = false;
  rigorous_memory_boundary_check_ = false;
  rigorous_page_boundary_check_ = false;
  page_pool_size_mb_per_node_ = kDefaultPagePoolSizeMbPerNode;
  private_page_pool_initial_grab_ = PagePoolOffsetChunk::kMaxSize / 2;
}

ErrorStack MemoryOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, use_numa_alloc_);
  EXTERNALIZE_LOAD_ELEMENT(element, interleave_numa_alloc_);
  EXTERNALIZE_LOAD_ELEMENT(element, use_mmap_hugepages_);
  EXTERNALIZE_LOAD_ELEMENT(element, rigorous_memory_boundary_check_);
  EXTERNALIZE_LOAD_ELEMENT(element, rigorous_page_boundary_check_);
  EXTERNALIZE_LOAD_ELEMENT(element, page_pool_size_mb_per_node_);
  EXTERNALIZE_LOAD_ELEMENT(element, private_page_pool_initial_grab_);
  return kRetOk;
}

ErrorStack MemoryOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for memory manager"));

  EXTERNALIZE_SAVE_ELEMENT(element, use_numa_alloc_,
    "Whether to use ::numa_alloc_interleaved()/::numa_alloc_onnode() to allocate memories"
    " in NumaCoreMemory and NumaNodeMemory.\n"
    " If false, we use usual posix_memalign() instead.\n"
    " If everything works correctly, ::numa_alloc_interleaved()/::numa_alloc_onnode()\n"
    " should result in much better performance because each thread should access only\n"
    " the memories allocated for the NUMA node. Default is true..");
  EXTERNALIZE_SAVE_ELEMENT(element, interleave_numa_alloc_,
    "Whether to use ::numa_alloc_interleaved() instead of ::numa_alloc_onnode()\n"
    " If everything works correctly, numa_alloc_onnode() should result in much better\n"
    " performance because interleaving just wastes memory if it is very rare to access other\n"
    " node's memory. Default is false.\n"
    " If use_numa_alloc_ is false, this configuration has no meaning.");
  EXTERNALIZE_SAVE_ELEMENT(element, use_mmap_hugepages_,
    "Whether to use non-transparent hugepages for big memories (1GB huge pages)\n"
    " To use this, you have to set up non-transparent hugepages that requires a reboot.\n"
    " See the readme fore more details.");
  EXTERNALIZE_SAVE_ELEMENT(element, rigorous_memory_boundary_check_,
    "Whether to use mprotect() for memory boundaries to detect bogus memory accesses.");
  EXTERNALIZE_SAVE_ELEMENT(element, rigorous_page_boundary_check_,
    "Whether to use mprotect() for page boundaries to detect bogus memory accesses.");
  EXTERNALIZE_SAVE_ELEMENT(element, page_pool_size_mb_per_node_,
          "Size of the page pool in MB per each NUMA node. Must be multiply of 2MB.");
  EXTERNALIZE_SAVE_ELEMENT(element, private_page_pool_initial_grab_,
    "How many pages each NumaCoreMemory initially grabs when it is initialized."
    " Default is 50% of PagePoolOffsetChunk::kMaxSize\n"
    " Obviously, private_page_pool_initial_grab_ * kPageSize * number-of-threads must be"
    " within page_pool_size_mb_per_node_ to start up the engine.");
  return kRetOk;
}

}  // namespace memory
}  // namespace foedus
