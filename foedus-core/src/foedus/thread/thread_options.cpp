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
#include "foedus/thread/thread_options.hpp"

#include <numa.h>

#include <thread>

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace thread {
ThreadOptions::ThreadOptions() {
  int total_cores;
  if (::numa_available() < 0) {
    group_count_ = 1;
    total_cores = std::thread::hardware_concurrency();  // seems to use pthread_num_processors_np?
  } else {
    group_count_ = ::numa_num_configured_nodes();
    if (group_count_ == 0) {
      group_count_ = 1;
    }

    total_cores = ::numa_num_configured_cpus();
    if (total_cores == 0) {
      total_cores = 1;
    }
  }
  thread_count_per_group_ = total_cores / group_count_;
  overwrite_thread_schedule_ = false;
  thread_policy_ = kScheduleFifo;
  thread_priority_ = kPriorityDefault;
}

ErrorStack ThreadOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, group_count_);
  EXTERNALIZE_LOAD_ELEMENT(element, thread_count_per_group_);
  EXTERNALIZE_LOAD_ELEMENT(element, overwrite_thread_schedule_);
  EXTERNALIZE_LOAD_ENUM_ELEMENT(element, thread_policy_);
  EXTERNALIZE_LOAD_ENUM_ELEMENT(element, thread_priority_);
  return kRetOk;
}

ErrorStack ThreadOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options about threads and thread-groups"));

  EXTERNALIZE_SAVE_ELEMENT(element, group_count_,
    "Number of ThreadGroup in the engine.\n"
    " Default value is hardware NUMA node count (::numa_num_configured_nodes()).");
  EXTERNALIZE_SAVE_ELEMENT(element, thread_count_per_group_,
    "Number of Thread in each ThreadGroup. Default value is hardware NUMA core count;\n"
    " ::numa_num_configured_cpus() / ::numa_num_configured_nodes()");
  EXTERNALIZE_SAVE_ELEMENT(element, overwrite_thread_schedule_,
    "Whether to overwrite policy/priority of worker threads.");
  EXTERNALIZE_SAVE_ENUM_ELEMENT(element, thread_policy_,
    "Thread policy for worker threads. ignored if overwrite_thread_schedule_==false\n"
    "The values are compatible with pthread's values.");
  EXTERNALIZE_SAVE_ENUM_ELEMENT(element, thread_priority_,
    "Thread priority for worker threads. ignored if overwrite_thread_schedule_==false\n"
    "The values are compatible with pthread's values.");
  return kRetOk;
}

}  // namespace thread
}  // namespace foedus
