/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <glog/logging.h>
#include <string>
#include <ostream>
#include <sstream>
#include <thread>
namespace foedus {
namespace thread {
void StoppableThread::initialize(const std::string &name,
    std::thread &&the_thread, const std::chrono::microseconds &sleep_interval) {
    name_ = name;
    thread_ = std::move(the_thread);
    sleep_interval_ = sleep_interval;
    started_ = true;
    stop_requested_ = false;
    stopped_ = false;
    LOG(INFO) << name_ << " initialized. sleep_interval=" << sleep_interval_.count() << " microsec";
}

void StoppableThread::initialize(const std::string& name_prefix, int32_t name_ordinal,
                        std::thread &&the_thread, const std::chrono::microseconds &sleep_interval) {
    std::stringstream str;
    str << name_prefix << name_ordinal;
    initialize(str.str(), std::move(the_thread), sleep_interval);
}

bool StoppableThread::sleep() {
    VLOG(1) << name_ << " sleeping for " << sleep_interval_.count() << " microsec";
    condition_.wait_for(sleep_interval_);
    VLOG(1) << name_ << " woke up";
    if (is_stop_requested()) {
        LOG(INFO) << name_ << " stop requested";
        return true;
    } else {
        return false;
    }
}

void StoppableThread::wakeup() {
    VLOG(1) << "Waking up " << name_ << "...";
    condition_.notify_one();
}

void StoppableThread::stop() {
    request_stop();
    wait_for_stop();
}
void StoppableThread::request_stop() {
    if (started_ && !is_stopped() && !is_stop_requested()) {
        LOG(INFO) << "Requesting to stop " << name_ << "...";
        condition_.notify_one([this]{ stop_requested_ = true; });
        LOG(INFO) << "Requested to stop " << name_;
    } else {
        LOG(INFO) << "Already requested to stop: " << name_;
    }
}

void StoppableThread::wait_for_stop() {
    if (started_ && !is_stopped()) {
        LOG(INFO) << "Stopping " << name_ << "...";
        thread_.join();
        LOG(INFO) << "Successfully Stopped " << name_;
        stopped_ = true;
    }
}

std::string StoppableThread::to_string() const {
    std::stringstream stream;
    stream << *this;
    return stream.str();
}

std::ostream& operator<<(std::ostream& o, const StoppableThread& v) {
    o << "<StoppableThread>"
        << "<name_>" << v.name_ << "</name_>"
        << "<native_thread_id>" << v.thread_.get_id() << "</native_thread_id>"
        << "<sleep_interval_>" << v.sleep_interval_.count() << "</sleep_interval_>"
        << "<started_>" << v.started_.load() << "</started_>"
        << "<stop_requested_>" << v.stop_requested_.load() << "</stop_requested_>"
        << "<stopped_>" << v.stopped_.load() << "</stopped_>"
        << "</StoppableThread>";
    return o;
}

}  // namespace thread
}  // namespace foedus
