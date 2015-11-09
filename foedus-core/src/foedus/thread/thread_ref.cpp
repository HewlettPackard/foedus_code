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
#include "foedus/thread/thread_ref.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/atomic_fences.hpp"
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
  mcs_blocks_(nullptr),
  mcs_rw_blocks_(nullptr) {}

ThreadRef::ThreadRef(Engine* engine, ThreadId id) : engine_(engine), id_(id) {
  soc::SharedMemoryRepo* memory_repo = engine->get_soc_manager()->get_shared_memory_repo();
  soc::ThreadMemoryAnchors* anchors = memory_repo->get_thread_memory_anchors(id);
  control_block_ = anchors->thread_memory_;
  task_input_memory_ = anchors->task_input_memory_;
  task_output_memory_ = anchors->task_output_memory_;
  mcs_blocks_ = anchors->mcs_lock_memories_;
  mcs_rw_blocks_ = anchors->mcs_rw_lock_memories_;
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
  if (UNLIKELY(control_block_->status_ == kNotInitialized)) {
    // The worker thread has not started working.
    // In this case, wait until it's initialized.
    while (control_block_->status_ == kNotInitialized) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      assorted::memory_fence_acquire();
    }
  }
  if (control_block_->status_ != kWaitingForTask) {
    DVLOG(0) << "(fast path) Someone already took Thread-" << id_ << ".";
    return false;
  }

  {
    // now, check it and grab it with mutex
    soc::SharedMutexScope scope(&control_block_->task_mutex_);
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
  }
  // waking up doesn't need mutex
  control_block_->wakeup_cond_.signal();
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

Epoch ThreadRef::get_in_commit_epoch() const {
  assorted::memory_fence_acquire();
  return control_block_->in_commit_epoch_;
}

uint64_t ThreadRef::get_snapshot_cache_hits() const {
  return control_block_->stat_snapshot_cache_hits_;
}

uint64_t ThreadRef::get_snapshot_cache_misses() const {
  return control_block_->stat_snapshot_cache_misses_;
}

void ThreadRef::reset_snapshot_cache_counts() const {
  control_block_->stat_snapshot_cache_hits_ = 0;
  control_block_->stat_snapshot_cache_misses_ = 0;
}

Epoch ThreadGroupRef::get_min_in_commit_epoch() const {
  assorted::memory_fence_acquire();
  Epoch ret = INVALID_EPOCH;
  for (const auto& t : threads_) {
    Epoch in_commit_epoch = t.get_control_block()->in_commit_epoch_;
    if (in_commit_epoch.is_valid()) {
      if (!ret.is_valid()) {
        ret = in_commit_epoch;
      } else {
        ret.store_min(in_commit_epoch);
      }
    }
  }

  return ret;
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
