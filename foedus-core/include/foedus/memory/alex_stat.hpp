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
#ifndef FOEDUS_MEMORY_ALEX_STAT_HPP_
#define FOEDUS_MEMORY_ALEX_STAT_HPP_

#include <stdint.h>

#include "foedus/assert_nd.hpp"
#include "foedus/storage/page.hpp"

namespace foedus {
namespace memory {

/** Only in alex_memstat branch. */
struct AlexStatBlock {
  uint32_t reads_;
  uint32_t writes_;
  uint32_t atomics_;
  uint32_t reserved_;
};

struct AlexStatPack {
  AlexStatPack() : memory_(0), blocks_per_node_(0), nodes_(0) {}
  AlexStatPack(AlexStatBlock* memory, uint32_t blocks_per_node, uint32_t nodes)
    : memory_(memory), blocks_per_node_(blocks_per_node), nodes_(nodes) {}

  AlexStatBlock* memory_;
  uint32_t blocks_per_node_;
  uint32_t nodes_;
};

inline AlexStatBlock& get_alex_block(
  AlexStatPack pack,
  uint16_t node,
  uint32_t block) {
  if (block < pack.blocks_per_node_ && node < pack.nodes_) {
    return pack.memory_[node * pack.blocks_per_node_ + block];
  } else {
    return pack.memory_[0];  // kind of dummy
  }
}

inline AlexStatBlock& get_alex_block_inpage(
  AlexStatPack pack,
  const void* somewhere_in_page) {
  storage::Page* page = storage::to_page(somewhere_in_page);
  storage::VolatilePagePointer pointer;
  pointer.word = page->get_header().page_id_;
  return get_alex_block(pack, pointer.components.numa_node, pointer.components.offset);
}

inline void add_alex_read(AlexStatPack pack, storage::VolatilePagePointer pointer) {
  ++get_alex_block(pack, pointer.components.numa_node, pointer.components.offset).reads_;
}
inline void add_alex_write(AlexStatPack pack, storage::VolatilePagePointer pointer) {
  ++get_alex_block(pack, pointer.components.numa_node, pointer.components.offset).writes_;
}
inline void add_alex_either(AlexStatPack pack, storage::VolatilePagePointer pointer, bool write) {
  if (write) {
    add_alex_write(pack, pointer);
  } else {
    add_alex_read(pack, pointer);
  }
}
inline void add_alex_atomic(AlexStatPack pack, storage::VolatilePagePointer pointer) {
  ++get_alex_block(pack, pointer.components.numa_node, pointer.components.offset).atomics_;
}

inline void add_alex_read_inpage(AlexStatPack pack, const void* somewhere_in_page) {
  ++get_alex_block_inpage(pack, somewhere_in_page).reads_;
}
inline void add_alex_write_inpage(AlexStatPack pack, const void* somewhere_in_page) {
  ++get_alex_block_inpage(pack, somewhere_in_page).writes_;
}
inline void add_alex_either_inpage(AlexStatPack pack, const void* somewhere_in_page, bool write) {
  if (write) {
    add_alex_write_inpage(pack, somewhere_in_page);
  } else {
    add_alex_read_inpage(pack, somewhere_in_page);
  }
}
inline void add_alex_atomic_inpage(AlexStatPack pack, const void* somewhere_in_page) {
  ++get_alex_block_inpage(pack, somewhere_in_page).atomics_;
}


}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_ALEX_STAT_HPP_
