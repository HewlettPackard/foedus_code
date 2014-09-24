/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/thread_ref.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/impersonate_session.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/thread/thread_pimpl.hpp"

namespace foedus {
namespace thread {

ThreadRef::ThreadRef()
  : engine_(nullptr),
  id_(0),
  control_block_(nullptr),
  task_input_memory_(nullptr),
  task_output_memory_(nullptr),
  mcs_blocks_(nullptr) {}

ThreadRef::ThreadRef(Engine* engine, ThreadId id) : engine_(engine), id_(id) {
  soc::SharedMemoryRepo* memory_repo = engine->get_soc_manager()->get_shared_memory_repo();
  soc::ThreadMemoryAnchors* anchors = memory_repo->get_thread_memory_anchors(id);
  control_block_ = anchors->thread_memory_;
  task_input_memory_ = anchors->task_input_memory_;
  task_output_memory_ = anchors->task_output_memory_;
  mcs_blocks_ = anchors->mcs_lock_memories_;
}

bool ThreadRef::try_impersonate(
  const proc::ProcName& proc_name,
  const void* task_input,
  uint64_t task_input_size,
  ImpersonateSession *session) {
  if (session->is_valid()) {
    LOG(WARNING) << "This session is already attached to some thread. Releasing the current one..";
    session->release();
  }
  if (control_block_->status_ != kWaitingForTask) {
    DVLOG(0) << "(fast path) Someone already took Thread-" << id_ << ".";
    return false;
  }

  // now, check it with mutex
  soc::SharedMutexScope scope(control_block_->wakeup_cond_.get_mutex());
  if (control_block_->status_ != kWaitingForTask) {
    DVLOG(0) << "(slow path) Someone already took Thread-" << id_ << ".";
    return false;
  }
  session->thread_ = this;
  session->ticket_ = ++control_block_->current_ticket_;
  control_block_->proc_name_ = proc_name;
  control_block_->status_ = kWaitingForExecution;
  control_block_->input_len_ = task_input_size;
  if (task_input_size > 0) {
    std::memcpy(task_input_memory_, task_input, task_input_size);
  }
  control_block_->wakeup_cond_.signal(&scope);
  VLOG(0) << "Impersonation succeeded for Thread-" << id_ << ".";
  return true;
}

ThreadGroupRef::ThreadGroupRef() : engine_(nullptr), group_id_(0) {
}

ThreadGroupRef::ThreadGroupRef(Engine* engine, ThreadGroupId group_id)
  : engine_(engine), group_id_(group_id) {
  uint16_t count = engine->get_options().thread_.thread_count_per_group_;
  for (uint16_t i = 0; i < count; ++i) {
    threads_.emplace_back(ThreadRef(engine, compose_thread_id(group_id, i)));
  }
}
std::ostream& operator<<(std::ostream& o, const ThreadGroupRef& v) {
  o << "<ThreadGroupRef>";
  o << "<group_id_>" << static_cast<int>(v.get_group_id()) << "</group_id_>";
  o << "<threads_>";
  for (const ThreadRef& child_thread : v.threads_) {
    o << child_thread;
  }
  o << "</threads_>";
  o << "</ThreadGroup>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const ThreadRef& v) {
  o << "ThreadRef-" << v.get_thread_id() << "[";
  o << "status=" << (v.get_control_block()->status_);
  o << "]";
  return o;
}


}  // namespace thread
}  // namespace foedus
