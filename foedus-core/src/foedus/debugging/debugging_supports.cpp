/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/debugging/debugging_supports.hpp"

#ifdef HAVE_PAPI
#include <papi.h>
#endif  // HAVE_PAPI

#include <glog/logging.h>
#include <glog/vlog_is_on.h>
#ifdef HAVE_GOOGLEPERFTOOLS
#include <google/profiler.h>
#endif  // HAVE_GOOGLEPERFTOOLS

#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"

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
    if (options.verbose_modules_.size() > 0) {
      // Watch out for this bug, if we get a crash here:
      // https://code.google.com/p/google-glog/issues/detail?id=172
      google::SetVLOGLevel(options.verbose_modules_.c_str(), options.verbose_log_level_);
    }
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
  std::memset(&papi_counters_, 0, sizeof(papi_counters_));
  papi_enabled_ = false;
  return kRetOk;
}
ErrorStack DebuggingSupports::uninitialize_once() {
  uninitialize_glog();  // release glog at the end. we can't use glog since now
  return kRetOk;
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
void DebuggingSupports::set_verbose_module(const std::string &module, int verbose) {
  // Watch out for this bug, if we get a crash here:
  // https://code.google.com/p/google-glog/issues/detail?id=172
  google::SetVLOGLevel(module.c_str(), verbose);
  LOG(INFO) << "Invoked google::SetVLOGLevel for " << module << ", level=" << verbose;
}


#ifdef HAVE_PAPI

#define X(a, b) a,
int kPapiEvents[] = {
#include "foedus/debugging/papi_events.xmacro"  // NOLINT
};
#undef X
const uint16_t kPapiEventCount = sizeof(kPapiEvents) / sizeof(int);

#define X_QUOTE(str) #str
#define X_EXPAND_AND_QUOTE(str) X_QUOTE(str)
#define X(a, b) X_EXPAND_AND_QUOTE(a) ": " b,
const char* kPapiEventNames[] = {
#include "foedus/debugging/papi_events.xmacro"  // NOLINT
};
#undef X
#undef X_EXPAND_AND_QUOTE
#undef X_QUOTE

void DebuggingSupports::start_papi_counters() {
  int total_counters = ::PAPI_num_counters();
  if (total_counters <= PAPI_OK) {
    LOG(ERROR) << "PAPI is not supported in this environment. PAPI version=" << PAPI_VERSION;
    return;
  }
  LOG(INFO) << "PAPI has " << total_counters << " counters. PAPI version=" << PAPI_VERSION;
  int ret = ::PAPI_start_counters(kPapiEvents, kPapiEventCount);
  if (ret != PAPI_OK) {
    LOG(ERROR) << "PAPI_start_counters failed. retval=" << ret;
  } else {
    LOG(INFO) << "Started counting " << kPapiEventCount << " performance events via PAPI";
    papi_enabled_ = true;
  }
}
void DebuggingSupports::stop_papi_counters() {
  if (papi_enabled_) {
    int ret = ::PAPI_stop_counters(papi_counters_.counters_, kPapiEventCount);
    if (ret != PAPI_OK) {
      LOG(ERROR) << "PAPI_stop_counters failed. retval=" << ret;
    }
  }
}
std::vector<std::string> DebuggingSupports::describe_papi_counters(const PapiCounters& counters) {
  std::vector<std::string> ret;
  for (uint16_t i = 0; i < kPapiEventCount; ++i) {
    ret.emplace_back(std::string(kPapiEventNames[i]) + ":" + std::to_string(counters.counters_[i]));
  }
  return ret;
}
#else  // HAVE_PAPI
void DebuggingSupports::start_papi_counters() {
  LOG(WARNING) << "libpapi was not linked. No PAPI profile is collected.";
}
void DebuggingSupports::stop_papi_counters() {}
std::vector<std::string> DebuggingSupports::describe_papi_counters(const PapiCounters& counters) {
  std::vector<std::string> ret;
  ret.emplace_back("libpapi was not linked. No PAPI profile is collected");
  return ret;
}
#endif  // HAVE_PAPI


ErrorStack DebuggingSupports::start_profile(const std::string& output_file, bool papi_counters) {
#ifdef HAVE_GOOGLEPERFTOOLS
  int ret = ::ProfilerStart(output_file.c_str());
  if (ret == 0) {
    LOG(ERROR) << "ProfilerStart() returned zero (an error). os_error=" << assorted::os_error();
    return ERROR_STACK(kErrorCodeDbgGperftools);
  }
#else  // HAVE_GOOGLEPERFTOOLS
  LOG(WARNING) << "Google perftools was not linked. No profile is provided. " << output_file;
#endif  // HAVE_GOOGLEPERFTOOLS

  if (papi_counters) {
    start_papi_counters();
  } else {
    papi_enabled_ = false;
  }
  return kRetOk;
}

void DebuggingSupports::stop_profile() {
#ifdef HAVE_GOOGLEPERFTOOLS
  ::ProfilerStop();
#endif  // HAVE_GOOGLEPERFTOOLS
  stop_papi_counters();
}

}  // namespace debugging
}  // namespace foedus
