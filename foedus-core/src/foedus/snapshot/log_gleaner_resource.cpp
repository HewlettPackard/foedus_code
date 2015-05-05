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
#include "foedus/snapshot/log_gleaner_resource.hpp"

namespace foedus {
namespace snapshot {

void LogGleanerResource::allocate(uint16_t node_count) {
  // Combining the root page info doesn't require much memory, so this size should be enough.
  work_memory_.alloc(1U << 21, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);

  // As construct_root() just writes out root pages, it won't require large buffer.
  // Especially, no buffer for intermediate pages required.
  writer_pool_memory_.alloc(1U << 21, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  writer_intermediate_memory_.alloc(1U << 12, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  tmp_root_page_memory_.alloc(1U << 12, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);

  for (uint16_t i = 0; i < node_count; ++i) {
    per_node_resources_.emplace_back(i);
  }
}

void LogGleanerResource::deallocate() {
  work_memory_.release_block();
  writer_pool_memory_.release_block();
  writer_intermediate_memory_.release_block();
  tmp_root_page_memory_.release_block();
  per_node_resources_.clear();
}

LogGleanerResource::PerNodeResource::PerNodeResource(uint16_t numa_node)
  : numa_node_(numa_node) {
  read_buffer_.alloc_onnode(
    kReadBufferInitialSize,
    1U << 21,
    numa_node);
  write_buffer_.alloc_onnode(
    kWriteBufferInitialSize,
    1U << 21,
    numa_node);
  write_intermediate_buffer_.alloc_onnode(
    kWriteBufferIntermediateInitialSize,
    1U << 21,
    numa_node);
}

}  // namespace snapshot
}  // namespace foedus
