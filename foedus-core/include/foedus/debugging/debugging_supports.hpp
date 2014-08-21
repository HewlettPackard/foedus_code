/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_DEBUGGING_DEBUGGING_SUPPORTS_HPP_
#define FOEDUS_DEBUGGING_DEBUGGING_SUPPORTS_HPP_

#include <string>
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/debugging/debugging_options.hpp"

namespace foedus {
namespace debugging {
/**
 * @brief APIs to support debugging functionalities.
 * @ingroup DEBUGGING
 */
class DebuggingSupports CXX11_FINAL : public DefaultInitializable {
 public:
  struct PapiCounters {
    /** wanna use int64_t, but to align with PAPI...*/
    long long int counters_[128];  // NOLINT[runtime/int](PAPI requirement)
  };

  DebuggingSupports() CXX11_FUNC_DELETE;
  explicit DebuggingSupports(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  /** @copydoc DebuggingOptions#debug_log_to_stderr_ */
  void                set_debug_log_to_stderr(bool value);
  /** @copydoc DebuggingOptions#debug_log_stderr_threshold_ */
  void                set_debug_log_stderr_threshold(DebuggingOptions::DebugLogLevel level);
  /** @copydoc DebuggingOptions#debug_log_min_threshold_ */
  void                set_debug_log_min_threshold(DebuggingOptions::DebugLogLevel level);
  /** @copydoc DebuggingOptions#verbose_log_level_ */
  void                set_verbose_log_level(int verbose);
  /** @copydoc DebuggingOptions#verbose_modules_ */
  void                set_verbose_module(const std::string &module, int verbose);

  /**
   * @brief Start running a CPU profiler (gperftools/PAPI).
   * @param[in] output_file path to output the profile result.
   * @param[in] papi_counters whether to also collect PAPI counters.
   * @details
   * This feature is enabled only when you link to libprofiler.so.
   * For example, use it like this:
   * @code{.cpp}
   * CHECK_ERROR(engine.get_debug().start_profile("hoge.prof"));
   * for (int i = 0; i < 1000000; ++i) do_something();
   * engine.get_debug().stop_profile("hoge.prof");
   * @endcode
   * Then, after the execution,
   * @code{.sh}
   * pprof --pdf your_binary hoge.prof > hoge.pdf
   * okular hoge.pdf
   * @endcode
   */
  ErrorStack          start_profile(const std::string& output_file, bool papi_counters = false);
  /** Stop CPU profiling. */
  void                stop_profile();

  /**
   * Returns the profiled PAPI counters. must be called \e after stop_profile().
   * You must call start_profile() with papi_counters=true.
   */
  const PapiCounters& get_papi_counters() const { return papi_counters_; }
  /**
   * Returns a human-readable explanation of PAPI counters.
   * One string for one counter to avoid returning too long string.
   */
  static std::vector<std::string> describe_papi_counters(const PapiCounters& counters);

 private:
  /**
   * Initialize Google-logging only once. This is called at the beginning of initialize_once()
   * so that all other initialization can use glog.
   */
  void                initialize_glog();
  /**
   * Uninitialize Google-logging only once.  This is called at the end of uninitialize_once()
   * so that all other uninitialization can use glog.
   */
  void                uninitialize_glog();
  void                start_papi_counters();
  void                stop_papi_counters();

  Engine* const           engine_;

  bool                    papi_enabled_;
  PapiCounters            papi_counters_;
};
}  // namespace debugging
}  // namespace foedus
#endif  // FOEDUS_DEBUGGING_DEBUGGING_SUPPORTS_HPP_
