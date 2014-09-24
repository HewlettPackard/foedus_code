/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_PROC_PROC_OPTIONS_HPP_
#define FOEDUS_PROC_PROC_OPTIONS_HPP_

#include <stdint.h>

#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/engine_type.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/fs/filesystem.hpp"

namespace foedus {
namespace proc {
/**
 * @brief Set of options for loading system/user procedures.
 * @ingroup PROC
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct ProcOptions CXX11_FINAL : public virtual externalize::Externalizable {
  enum Constants {
    /** Default value for max_proc_count_. */
    kDefaultMaxProcCount = 1 << 16,
  };
  /**
   * Constructs option values with default values.
   */
  ProcOptions();

  /** Maximum number of system/user procedures. */
  uint32_t      max_proc_count_;

  /**
   * @brief String pattern of ';'-separated path of shared libraries to load in each NUMA node.
   * @details
   * The default value is empty, which means we don't load any shared libraries.
   * If non-empty, we load the shared libraries of the path to register user-defined procedures.
   * A placeholder '$NODE$' is replaced with the NUMA node number.
   * For example, "/foo/bar/node_$NODE$/mylib.so becomes "/foo/bar/node_1/mylib.so" on node-1.
   * On the other hand, "/foo/bar/mylib.so becomes "/foo/bar/mylib.so" for all nodes.
   * The main purpose of using different binaries for each node is 1) to manually achieve
   * executable/library text replication on NUMA node, and 2) for kChildRemoteSpawned later.
   */
  fs::FixedPath shared_library_path_pattern_;

  /**
   * @brief String pattern of ';'-separated path of directories that contain
   * shared libaries to load.
   * @details
   * Similar to shared_library_path_pattern_. The difference is that all ".so" files under
   * the directory is loaded. The default value is empty.
   */
  fs::FixedPath shared_library_dir_pattern_;

  /** converts shared_library_path_pattern_ into a string with the given node ID. */
  std::string   convert_shared_library_path_pattern(int node) const;
  /** converts shared_library_dir_pattern_ into a string with the given node ID. */
  std::string   convert_shared_library_dir_pattern(int node) const;

  EXTERNALIZABLE(ProcOptions);
};
}  // namespace proc
}  // namespace foedus
#endif  // FOEDUS_PROC_PROC_OPTIONS_HPP_
