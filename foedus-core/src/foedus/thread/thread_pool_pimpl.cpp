/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/thread_pool_pimpl.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_group.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/thread/thread_options.hpp"
#include "foedus/thread/thread_pimpl.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace thread {
ErrorStack ThreadPoolPimpl::initialize_once() {
  if (!engine_->get_memory_manager().is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }
  ASSERT_ND(groups_.empty());
  const ThreadOptions &options = engine_->get_options().thread_;
  for (ThreadGroupId group_id = 0; group_id < options.group_count_; ++group_id) {
    groups_.emplace_back(ThreadGroupRef(engine_, group_id));
  }

  if (!engine_->is_master()) {
    // initialize local thread group object
    soc::SocId node = engine_->get_soc_id();
    local_group_ = new ThreadGroup(engine_, node);
    CHECK_ERROR(local_group_->initialize());
  }
  return kRetOk;
}

ErrorStack ThreadPoolPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  if (!engine_->get_memory_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }

  groups_.clear();
  if (local_group_) {
    ASSERT_ND(!engine_->is_master());
    batch.emprace_back(local_group_->uninitialize());
    delete local_group_;
    local_group_ = nullptr;
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}
ThreadRef* ThreadPoolPimpl::get_thread(ThreadId id) {
  return get_group(decompose_numa_node(id))->get_thread(decompose_numa_local_ordinal(id));
}


bool ThreadPoolPimpl::impersonate(
  const proc::ProcName& proc_name,
  const void* task_input,
  uint64_t task_input_size,
  ImpersonateSession *session) {
  uint16_t thread_per_group = engine_->get_options().thread_.thread_count_per_group_;
  for (ThreadGroupRef& group : groups_) {
    for (size_t j = 0; j < thread_per_group; ++j) {
      ThreadRef* thread = group.get_thread(j);
      if (thread->try_impersonate(proc_name, task_input, task_input_size, session)) {
        return true;
      }
    }
  }
  return false;
}
bool ThreadPoolPimpl::impersonate_on_numa_node(
  ThreadGroupId node,
  const proc::ProcName& proc_name,
  const void* task_input,
  uint64_t task_input_size,
  ImpersonateSession *session) {
  uint16_t thread_per_group = engine_->get_options().thread_.thread_count_per_group_;
  ThreadGroupRef& group = groups_[node];
  for (size_t j = 0; j < thread_per_group; ++j) {
    ThreadRef* thread = group.get_thread(j);
    if (thread->try_impersonate(proc_name, task_input, task_input_size, session)) {
      return true;
    }
  }
  return false;
}
bool ThreadPoolPimpl::impersonate_on_numa_core(
  ThreadId core,
  const proc::ProcName& proc_name,
  const void* task_input,
  uint64_t task_input_size,
  ImpersonateSession *session) {
  ThreadRef* thread = get_thread(core);
  return thread->try_impersonate(proc_name, task_input, task_input_size, session);
}

std::ostream& operator<<(std::ostream& o, const ThreadPoolPimpl& v) {
  o << "<ThreadPool>";
  o << "<groups>";
  /*
  for (ThreadGroupRef& group : v.groups_) {
    o << group;
  }
  */
  o << "</groups>";
  o << "</ThreadPool>";
  return o;
}

}  // namespace thread
}  // namespace foedus
