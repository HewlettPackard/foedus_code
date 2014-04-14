/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/impersonate_session.hpp>
#include <foedus/thread/thread.hpp>
#include <chrono>
#include <future>
#include <iostream>
namespace foedus {
namespace thread {

static_assert(sizeof(ImpersonateSession::result_future_) <= sizeof(std::shared_future<ErrorStack>),
    "size of std::shared_future<ErrorStack> is larger than expected.");

std::shared_future<ErrorStack>* as_future(char* ptr) {
    return reinterpret_cast< std::shared_future< ErrorStack >* >(ptr);
}
const std::shared_future<ErrorStack>* as_future(const char* ptr) {
    return reinterpret_cast<const std::shared_future< ErrorStack >* >(ptr);
}

ImpersonateSession::ImpersonateSession(ImpersonateTask* task)
    : thread_(nullptr), task_(task), invalid_cause_() {
    // to avoid pimpl's new/delete, we use aligned storage for future.
    new (result_future_) std::shared_future<ErrorStack>();
}
ImpersonateSession::ImpersonateSession(const ImpersonateSession& other)
    : thread_(other.thread_), task_(other.task_), invalid_cause_(other.invalid_cause_) {
    new (result_future_) std::shared_future<ErrorStack>(*as_future(other.result_future_));
}
ImpersonateSession::~ImpersonateSession() {
    as_future(result_future_)->~shared_future();
}
ImpersonateSession& ImpersonateSession::operator=(const ImpersonateSession& other) {
    thread_ = other.thread_;
    task_ = other.task_;
    invalid_cause_ = other.invalid_cause_;
    as_future(result_future_)->operator=(*as_future(other.result_future_));
    return *this;
}

ImpersonateSession::ImpersonateSession(ImpersonateSession&& other)
    : thread_(other.thread_), task_(other.task_), invalid_cause_(other.invalid_cause_) {
    new (result_future_) std::shared_future<ErrorStack>(
        std::move(*as_future(other.result_future_)));
}

ImpersonateSession& ImpersonateSession::operator=(ImpersonateSession&& other) {
    thread_ = other.thread_;
    task_ = other.task_;
    invalid_cause_ = other.invalid_cause_;
    as_future(result_future_)->operator=(std::move(*as_future(other.result_future_)));
    return *this;
}

ErrorStack ImpersonateSession::get_result() {
    assert(is_valid());
    return as_future(result_future_)->get();
}
void ImpersonateSession::wait() const {
    assert(is_valid());
    return as_future(result_future_)->wait();
}
ImpersonateSession::Status ImpersonateSession::wait_for(TimeoutMicrosec timeout) const {
    if (!is_valid()) {
        return ImpersonateSession::INVALID_SESSION;
    } else if (timeout < 0) {
        // this means unconditional wait.
        wait();
        return ImpersonateSession::READY;
    } else {
        std::future_status status = as_future(result_future_)->wait_for(
                std::chrono::microseconds(timeout));
        if (status == std::future_status::timeout) {
            return ImpersonateSession::TIMEOUT;
        } else {
            assert(status == std::future_status::ready);
            return ImpersonateSession::READY;
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
