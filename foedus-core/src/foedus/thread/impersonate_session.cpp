/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/impersonate_session.hpp>
#include <foedus/thread/impersonate_task.hpp>
#include <foedus/thread/impersonate_task_pimpl.hpp>
#include <foedus/thread/thread.hpp>
#include <chrono>
#include <iostream>
namespace foedus {
namespace thread {

ErrorStack ImpersonateSession::get_result() {
  ASSERT_ND(is_valid());
  wait();
  return task_->pimpl_->result_;
}
void ImpersonateSession::wait() const {
  ASSERT_ND(is_valid());
  task_->pimpl_->rendezvous_.wait();
}
ImpersonateSession::Status ImpersonateSession::wait_for(TimeoutMicrosec timeout) const {
  if (!is_valid()) {
    return ImpersonateSession::kInvalidSession;
  } else if (timeout < 0) {
    // this means unconditional wait.
    wait();
    return ImpersonateSession::kReady;
  } else {
    bool done = task_->pimpl_->rendezvous_.wait_for(std::chrono::microseconds(timeout));
    if (!done) {
      return ImpersonateSession::kTimeout;
    } else {
      return ImpersonateSession::kReady;
    }
  }
}

std::ostream& operator<<(std::ostream& o, const ImpersonateSession& v) {
  o << "ImpersonateSession: valid=" << v.is_valid();
  if (v.is_valid()) {
    o << ", thread_id=" << v.thread_->get_thread_id() << ", task address=" << v.task_;
  } else {
    o << ", invalid_cause=" << v.invalid_cause_;
  }
  return o;
}

}  // namespace thread
}  // namespace foedus
