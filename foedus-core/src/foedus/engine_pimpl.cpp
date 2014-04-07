/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_pimpl.hpp>
#include <glog/logging.h>
#include <atomic>
#include <cassert>
namespace foedus {

std::atomic<int>    EnginePimpl::static_glog_initialize_counter = ATOMIC_VAR_INIT(0);
bool                EnginePimpl::static_glog_initialize_done = false;

EnginePimpl::EnginePimpl(const EngineOptions &options)
    : options_(options), memory_(nullptr), filesystem_(nullptr), initialized_(false) {
}
EnginePimpl::~EnginePimpl() {
}

void EnginePimpl::initialize_glog() {
    int old_value = std::atomic_fetch_add(&static_glog_initialize_counter, 1);
    assert(old_value >= 0);
    if (old_value == 0) {
        while (static_glog_initialize_done) {
            // surprising, race with someone who has just released glog. let's wait.
            std::atomic_thread_fence(std::memory_order_acquire);
        }
        assert(!static_glog_initialize_done);
        google::InitGoogleLogging("libfoedus");
        static_glog_initialize_done = true;
        std::atomic_thread_fence(std::memory_order_release);  // announce the change to others
    } else {
        while (!static_glog_initialize_done) {
            // oh, racy. spin until it's done.
            std::atomic_thread_fence(std::memory_order_acquire);
        }
    }
}

void EnginePimpl::uninitialize_glog() {
    int old_value = std::atomic_fetch_sub(&static_glog_initialize_counter, 1);
    assert(old_value >= 1);
    if (old_value == 1) {
        assert(static_glog_initialize_done);
        google::ShutdownGoogleLogging();
        static_glog_initialize_done = false;
        std::atomic_thread_fence(std::memory_order_release);  // announce the change to others
    }
}

ErrorStack EnginePimpl::initialize_once() {
    initialize_glog();  // initialize glog at the beginning. we can use glog since now
    return RET_OK;
}
ErrorStack EnginePimpl::uninitialize_once() {
    uninitialize_glog();  // release glog at the end. we can't use glog since now
    return RET_OK;
}
}  // namespace foedus
