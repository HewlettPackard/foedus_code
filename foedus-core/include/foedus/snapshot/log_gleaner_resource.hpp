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
#ifndef FOEDUS_SNAPSHOT_LOG_GLEANER_RESOURCE_HPP_
#define FOEDUS_SNAPSHOT_LOG_GLEANER_RESOURCE_HPP_

#include <stdint.h>

#include <vector>

#include "foedus/memory/aligned_memory.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief Local resource for the log gleaner, which runs only in the master node.
 * Empty in child nodes.
 * @ingroup SNAPSHOT
 */
struct LogGleanerResource {
  enum Constants {
    kReadBufferInitialSize = 1 << 21,
    kWriteBufferInitialSize = 1 << 21,
    kWriteBufferIntermediateInitialSize = 1 << 21,
  };
  void allocate(uint16_t node_count);
  void deallocate();

  /**
   * These buffers are used to read intermediate results from each reducer to compose the
   * root page or other kinds of pages that weren't composed in each reducer (eg. all intermediate
   * nodes in hash storages).
   * Such a post-composition work is also parallelized and pinned to the NUMA node, so
   * we maintain soc_count buffers. The index is NUMA node.
   * The size of individual buffers might automatically expand.
   */
  struct PerNodeResource {
    explicit PerNodeResource(uint16_t numa_node);
    /** used for reading intermediate results frome each reducer. */
    memory::AlignedMemory read_buffer_;
    /** used for writing out pages that have no children */
    memory::AlignedMemory write_buffer_;
    /** used for writing out pages that have children */
    memory::AlignedMemory write_intermediate_buffer_;

    const uint16_t        numa_node_;
  };

  // these are for the snapshot writer in gleaner's construct_root()
  /** Working memory to be used in gleaner's construct_root(). Automatically expand if needed. */
  memory::AlignedMemory work_memory_;
  memory::AlignedMemory writer_pool_memory_;
  memory::AlignedMemory writer_intermediate_memory_;
  memory::AlignedMemory tmp_root_page_memory_;

  std::vector< PerNodeResource >  per_node_resources_;
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_GLEANER_RESOURCE_HPP_
