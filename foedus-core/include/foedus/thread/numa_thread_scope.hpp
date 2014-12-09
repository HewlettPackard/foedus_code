/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_NUMA_THREAD_SCOPE_HPP_
#define FOEDUS_THREAD_NUMA_THREAD_SCOPE_HPP_

#include <numa.h>

#include "foedus/assorted/mod_numa_node.hpp"

namespace foedus {
namespace thread {
/**
 * @brief Pin the current thread to the given NUMA node in this object's scope.
 * @ingroup THREAD MEMHIERARCHY
 * @details
 * Declare this object as soon as the thread starts.
 */
struct NumaThreadScope {
  explicit NumaThreadScope(int numa_node) {
    if (::numa_available() >= 0) {
      numa_node = assorted::mod_numa_node(numa_node);
      ::numa_run_on_node(numa_node);
      ::numa_set_localalloc();
    }
  }
  ~NumaThreadScope() {
    if (::numa_available() >= 0) {
      ::numa_run_on_node_mask(::numa_all_nodes_ptr);
    }
  }
};

}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_NUMA_THREAD_SCOPE_HPP_
