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
#include <stdint.h>

#include <iostream>

#include "foedus/assorted/uniform_random.hpp"
#include "foedus/sssp/sssp_common.hpp"

int main(int /*argc*/, char **/*argv*/) {
  foedus::assorted::UniformRandom rnd_(123456);
  for (uint32_t i = 0; i < (1U << 21);) {
    uint32_t in_partition_node = rnd_.next_uint32() % foedus::sssp::kNodesPerPartition;
    foedus::sssp::NodeId source_id = in_partition_node;

    // Then, determine the destination node. So far we blindly add 64 (2 blocks away).
    // If this results in more than max_node_id_, we must retry.
    foedus::sssp::NodeId dest_id = source_id + 64;
    if (dest_id >= foedus::sssp::kNodesPerPartition) {
      continue;
    }

    // Also, this occasionally picks a too-far node for a navigational query.
    // It happens when adding 64 changes its "by". Let's retry in that case too.
    const uint64_t kNodePerBy = foedus::sssp::kNodesPerBlock * foedus::sssp::kPartitionSize;
    if ((in_partition_node % kNodePerBy) >= kNodePerBy - 64) {
      continue;
    }

    std::cout << source_id << "," << dest_id << std::endl;

    ++i;
  }
}
