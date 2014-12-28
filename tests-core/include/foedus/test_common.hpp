/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TEST_COMMON_HPP_
#define FOEDUS_TEST_COMMON_HPP_

#include <string>

#include "foedus/engine_options.hpp"
#include "foedus/fs/fwd.hpp"

namespace foedus {
  /**
   * Returns one randomly generated name in "%%%%_%%%%_%%%%_%%%%" format.
   */
  std::string     get_random_name();

  /** Constructs a file path of the given file name using a randomly generated folder. */
  std::string     get_random_tmp_file_path(const std::string& name);

  /**
   * Constructs an EngineOption so that all file paths are unique random.
   * This makes it possible to run an arbitrary number of tests in parallel.
   */
  EngineOptions   get_randomized_paths(int logger_count, int snapshot_folder_count);

  /**
   * Use this for most testcases to reduce test execution time.
   */
  EngineOptions   get_tiny_options();

  /** Delete all files under the folder starting with the given prefix. */
  void            remove_files_start_with(const fs::Path &folder, const fs::Path &prefix);

  /**
   * Deletes all files created by the engine. Best effort.
   */
  void            cleanup_test(const EngineOptions& options);

  /**
   * Call this method at the beginning of a testcase that requires more than 1 NUMA node.
   * All testcases have to pass even in a single-NUMA node machine!
   */
  bool            is_multi_nodes();

  /**
   * Register signal handlers to capture signals during testcase execution.
   */
  void            register_signal_handlers(
    const char* test_case_name,
    const char* package_name,
    int argc,
    char** argv);

  /**
   * As the name suggests, we write out an gtest's result xml file with error state so that
   * jenkins will get aware of some error if the process disappears without any trace,
   * for example ctest killed it (via SIGSTOP, which can't be captured) for timeout.
   */
  void            pre_populate_error_result_xml();
}  // namespace foedus

#define TEST_QUOTE(str) #str
#define TEST_EXPAND_AND_QUOTE(str) TEST_QUOTE(str)

/**
 * Put this macro to define a main() that registers signal handlers.
 * This is required to convert assertion failures (crashes) to failed tests and provide more
 * detailed information in google-test's result xml file.
 * This really should be a built-in feature in gtest...
 *
 * But, I'm not sure if I should blame ctest, jenkins, or gtest (or all of them).
 * Related URLs:
 *   https://groups.google.com/forum/#!topic/googletestframework/NK5cAEqsioY
 *   https://code.google.com/p/googletest/issues/detail?id=342
 *   https://code.google.com/p/googletest/issues/detail?id=311
 */
#define TEST_MAIN_CAPTURE_SIGNALS(test_case_name, package_name) \
  int main(int argc, char **argv) { \
    foedus::register_signal_handlers( \
      TEST_EXPAND_AND_QUOTE(test_case_name), \
      TEST_EXPAND_AND_QUOTE(package_name), \
      argc, \
      argv); \
    foedus::pre_populate_error_result_xml(); \
    ::testing::InitGoogleTest(&argc, argv); \
    return RUN_ALL_TESTS(); \
  }

#endif  // FOEDUS_TEST_COMMON_HPP_
