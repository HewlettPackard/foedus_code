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
}  // namespace foedus

#endif  // FOEDUS_TEST_COMMON_HPP_
