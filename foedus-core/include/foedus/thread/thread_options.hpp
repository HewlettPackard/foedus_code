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
#ifndef FOEDUS_THREAD_THREAD_OPTIONS_HPP_
#define FOEDUS_THREAD_THREAD_OPTIONS_HPP_
#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/thread/thread_id.hpp"
namespace foedus {
namespace thread {
/**
 * @brief Set of options about threads and thread-groups.
 * @ingroup THREAD
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct ThreadOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /**
   * Constructs option values with default values.
   */
  ThreadOptions();

  /**
   * Number of ThreadGroup in the engine.
   * Default value is hardware NUMA node count (::numa_num_configured_nodes()).
   * Note that the type of this value is NOT ThreadGroupId so that we can handle 256 NUMA nodes.
   * Otherwise 256 becomes 0.
   */
  uint16_t                group_count_;

  /**
   * Number of Thread in each ThreadGroup.
   * Default value is hardware NUMA core count;
   * ::numa_num_configured_cpus() / ::numa_num_configured_nodes().
   */
  ThreadLocalOrdinal      thread_count_per_group_;

  /** Whether to overwrite policy/priority of worker threads. default false. */
  bool                    overwrite_thread_schedule_;

  /** Thread policy for worker threads. ignored if overwrite_thread_schedule_==false */
  ThreadPolicy            thread_policy_;
  /** Thread priority for worker threads. ignored if overwrite_thread_schedule_==false */
  ThreadPriority          thread_priority_;

  EXTERNALIZABLE(ThreadOptions);

  ThreadId                get_total_thread_count() const {
    return group_count_ * thread_count_per_group_;
  }
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_OPTIONS_HPP_
