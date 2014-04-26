/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/debugging/debugging_supports.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <glog/logging.h>
#include <mutex>
#include <string>
namespace foedus {
namespace debugging {
/**
 * @brief This and static_glog_initialize_lock are the \b only static variables
 * we have in the entire code base.
 * @details
 * Because google-logging requires initialization/uninitialization only once in a process,
 * we need this to coordinate it between multiple engines.
 * We increment/decrement this after taking lock on static_glog_initialize_lock.
 * The one who observed "0" as old value on increment, will initialize glog.
 * The one who observed "1" as old value on decrement, will uninitialize glog.
 * @invariant 0 or larger. negative value is definitely a bug in synchronization code.
 */
int         static_glog_initialize_counter = 0;

/**
 * @brief Exclusive lock variable for Google-logging's initialization/uninitialization.
 * @details
 * Each thread takes this mutex while init/uninit glog.
 */
std::mutex  static_glog_initialize_lock;

void DebuggingSupports::initialize_glog() {
    std::lock_guard<std::mutex> guard(static_glog_initialize_lock);  // implies fence too
    ASSERT_ND(static_glog_initialize_counter >= 0);
    if (static_glog_initialize_counter == 0) {
        // Set the glog configurations.
        const DebuggingOptions &options = engine_->get_options().debugging_;
        FLAGS_logtostderr = options.debug_log_to_stderr_;
        FLAGS_stderrthreshold = static_cast<int>(options.debug_log_stderr_threshold_);
        FLAGS_minloglevel = static_cast<int>(options.debug_log_min_threshold_);
        FLAGS_log_dir = options.debug_log_dir_;  // This one must be BEFORE InitGoogleLogging()
        FLAGS_v = options.verbose_log_level_;
        // TODO(Hideaki) ??? how to set FLAGS_vmodule?
        google::InitGoogleLogging("libfoedus");
        LOG(INFO) << "initialize_glog(): Initialized GLOG";
    } else {
        LOG(INFO) << "initialize_glog(): Observed that someone else has initialized GLOG";
    }
    ++static_glog_initialize_counter;
}

void DebuggingSupports::uninitialize_glog() {
    std::lock_guard<std::mutex> guard(static_glog_initialize_lock);  // implies fence too
    ASSERT_ND(static_glog_initialize_counter >= 1);
    if (static_glog_initialize_counter == 1) {
        LOG(INFO) << "uninitialize_glog(): Uninitializing GLOG...";
        google::ShutdownGoogleLogging();
    } else {
        LOG(INFO) << "uninitialize_glog(): There are still some other GLOG user.";
    }
    --static_glog_initialize_counter;
}

ErrorStack DebuggingSupports::initialize_once() {
    initialize_glog();  // initialize glog at the beginning. we can use glog since now
    return RET_OK;
}
ErrorStack DebuggingSupports::uninitialize_once() {
    uninitialize_glog();  // release glog at the end. we can't use glog since now
    return RET_OK;
}

void DebuggingSupports::set_debug_log_to_stderr(bool value) {
    FLAGS_logtostderr = value;
    LOG(INFO) << "Changed glog's FLAGS_logtostderr to " << value;
}
void DebuggingSupports::set_debug_log_stderr_threshold(DebuggingOptions::DebugLogLevel level) {
    FLAGS_stderrthreshold = static_cast<int>(level);
    LOG(INFO) << "Changed glog's FLAGS_stderrthreshold to " << level;
}
void DebuggingSupports::set_debug_log_min_threshold(DebuggingOptions::DebugLogLevel level) {
    FLAGS_minloglevel = static_cast<int>(level);
    LOG(INFO) << "Changed glog's FLAGS_minloglevel to " << level;
}
void DebuggingSupports::set_verbose_log_level(int verbose) {
    FLAGS_v = verbose;
    LOG(INFO) << "Changed glog's FLAGS_v to " << verbose;
}
void DebuggingSupports::set_verbose_modules(const std::string &modules) {
    // TODO(Hideaki) ??? how to set FLAGS_vmodule?
    LOG(INFO) << "Changed glog's FLAGS_??? to " << modules;
}


}  // namespace debugging
}  // namespace foedus
