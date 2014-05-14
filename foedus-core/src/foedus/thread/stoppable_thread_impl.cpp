/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <glog/logging.h>
#include <string>
#include <sstream>
#include <thread>
namespace foedus {
namespace thread {
void StoppableThread::initialize(const std::string &name,
    std::thread &&the_thread, const std::chrono::microseconds &sleep_interval) {
    name_ = name;
    thread_ = std::move(the_thread);
    sleep_interval_ = sleep_interval;
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
    std::unique_lock<std::mutex> the_lock(mutex_);
    condition_.wait_for(the_lock, sleep_interval_);
    VLOG(1) << name_ << " woke up";
    if (stop_requested_) {
        LOG(INFO) << name_ << " stop requested";
        return true;
    } else {
        return false;
    }
}

void StoppableThread::wakeup() {
    VLOG(1) << "Waking up " << name_ << "...";
    condition_.notify_all();
}


void StoppableThread::stop() {
    LOG(INFO) << "Stopping " << name_ << "...";
    assorted::memory_fence_acq_rel();
    if (!stopped_ && !stop_requested_) {
        stop_requested_ = true;
        assorted::memory_fence_release();
        condition_.notify_all();
        thread_.join();
        LOG(INFO) << "Joined " << name_;
    }
    stopped_ = true;
    assorted::memory_fence_release();
    LOG(INFO) << "Successfully Stopped " << name_;
}

}  // namespace thread
}  // namespace foedus
