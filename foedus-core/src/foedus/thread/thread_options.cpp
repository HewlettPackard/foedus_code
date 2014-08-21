/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/thread_options.hpp"

#include <numa.h>

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace thread {
ThreadOptions::ThreadOptions() {
  group_count_ = ::numa_num_configured_nodes();
  if (group_count_ == 0) {
    group_count_ = 1;
  }

  int total_cores = numa_num_configured_cpus();
  if (total_cores == 0) {
    total_cores = 1;
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
