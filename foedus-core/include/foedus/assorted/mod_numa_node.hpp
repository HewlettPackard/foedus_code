/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  int hardware_nodes = ::numa_num_configured_nodes();
  if (numa_node >= 0 && numa_node >= hardware_nodes && hardware_nodes > 0) {
    numa_node = numa_node % hardware_nodes;
  }
  return numa_node;
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_MOD_NUMA_NODE_HPP_
