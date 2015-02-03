/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/impersonate_session.hpp"

#include <chrono>
#include <iostream>

#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pimpl.hpp"
#include "foedus/thread/thread_ref.hpp"

namespace foedus {
namespace thread {

ImpersonateSession::ImpersonateSession(ImpersonateSession&& other) {
  ticket_ = other.ticket_;
  thread_ = other.thread_;
  other.ticket_ = 0;
  other.thread_ = nullptr;
}

ImpersonateSession& ImpersonateSession::operator=(ImpersonateSession&& other) {
  ticket_ = other.ticket_;
  thread_ = other.thread_;
  other.ticket_ = 0;
  other.thread_ = nullptr;
  return *this;
}


ErrorStack ImpersonateSession::get_result() {
  wait();
  if (is_valid()) {
    ThreadControlBlock* block = thread_->get_control_block();
    if (block->current_ticket_ != ticket_ || block->status_ != kWaitingForClientRelease) {
      return ERROR_STACK(kErrorCodeSessionExpired);
    }
    return block->proc_result_.to_error_stack();
  } else {
    return ERROR_STACK(kErrorCodeSessionExpired);
  }
}
void ImpersonateSession::get_output(void* output_buffer) {
  std::memcpy(output_buffer, thread_->get_task_output_memory(), get_output_size());
}
const void* ImpersonateSession::get_raw_output_buffer() {
  return thread_->get_task_output_memory();
}

uint64_t ImpersonateSession::get_output_size() {
  return thread_->get_control_block()->output_len_;
}

void ImpersonateSession::wait() const {
  if (!is_valid() || !is_running()) {
    return;
  }
  ThreadControlBlock* block = thread_->get_control_block();
  while (is_running()) {
    uint64_t demand = block->task_complete_cond_.acquire_ticket();
    if (!is_running()) {
      break;
    }
    block->task_complete_cond_.timedwait(demand, 100000ULL);
  }
}

bool ImpersonateSession::is_running() const {
  ASSERT_ND(thread_->get_control_block()->current_ticket_ >= ticket_);
  if (thread_->get_control_block()->current_ticket_ != ticket_) {
    return false;
  }
  return (thread_->get_control_block()->status_ == kWaitingForExecution ||
    thread_->get_control_block()->status_ == kRunningTask);
}

void ImpersonateSession::release() {
  if (!is_valid()) {
    return;
  }

  wait();
  ThreadControlBlock* block = thread_->get_control_block();
  if (block->current_ticket_ == ticket_ && block->status_ == kWaitingForClientRelease) {
    block->status_ = kWaitingForTask;
  }

  ticket_ = 0;
  thread_ = nullptr;
}


std::ostream& operator<<(std::ostream& o, const ImpersonateSession& v) {
  o << "ImpersonateSession: valid=" << v.is_valid();
  if (v.is_valid()) {
    o << ", thread_id=" << v.thread_->get_thread_id();
  }
  return o;
}

}  // namespace thread
}  // namespace foedus
