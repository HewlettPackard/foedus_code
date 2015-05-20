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
