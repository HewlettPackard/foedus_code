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
#ifndef FOEDUS_ASSORTED_MOD_NUMA_NODE_HPP_
#define FOEDUS_ASSORTED_MOD_NUMA_NODE_HPP_

#include <numa.h>

namespace foedus {
namespace assorted {

/**
 * In order to run even on a non-numa machine or a machine with fewer sockets,
 * we allow specifying arbitrary numa_node. we just take mod.
 * @ingroup ASSORTED
 */
inline int mod_numa_node(int numa_node) {
  // if the machine is not a NUMA machine (1-socket), then avoid calling libnuma functions.
  if (::numa_available() < 0) {
    return 0;
  }
  int hardware_nodes = ::numa_num_configured_nodes();
  if (numa_node >= 0 && numa_node >= hardware_nodes && hardware_nodes > 0) {
    numa_node = numa_node % hardware_nodes;
  }
  return numa_node;
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_MOD_NUMA_NODE_HPP_
