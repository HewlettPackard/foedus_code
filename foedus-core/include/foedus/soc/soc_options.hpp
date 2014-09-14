/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SOC_OPTIONS_HPP_
#define FOEDUS_SOC_SOC_OPTIONS_HPP_

#include <stdint.h>

#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/engine_type.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/fs/filesystem.hpp"

namespace foedus {
namespace soc {
/**
 * @brief Set of options for SOC manager.
 * @ingroup SOC
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct SocOptions CXX11_FINAL : public virtual externalize::Externalizable {
  enum Constants {
    /** default for shared_user_memory_size_kb_ */
    kDefaultSharedUserMemorySizeKb = 4,
  };

  /**
   * Constructs option values with default values.
   */
  SocOptions();

  /**
   * @brief How to launch SOC engine instances.
   * @details
   * The default value is kChildEmulated.
   */
  EngineType    soc_type_;

  /**
   * As part of the global shared memory, we reserve this size of 'user memory' that can be
   * used for arbitrary purporses by the user to communicate between SOCs.
   */
  uint64_t      shared_user_memory_size_kb_;

  /**
   * @brief String pattern of path of executables to spawn SOC engines in each NUMA node.
   * @details
   * The default value is empty, which means we use the binary of the master ("/proc/self/exe").
   * If non-empty, we use the path to launch each SOC engine.
   * A placeholder '$NODE$' is replaced with the NUMA node number.
   * For example, "/foo/bar/node_$NODE$/my_exec becomes "/foo/bar/node_1/my_exec" on node-1.
   * On the other hand, "/foo/bar/my_exec becomes "/foo/bar/my_exec" for all nodes.
   * The main purpose of using different binaries for each SOC is 1) to manually achieve
   * executable/library text replication on NUMA node, and 2) for kChildRemoteSpawned later.
   *
   * If soc_type_ is not kChildLocalSpawned or kChildRemoteSpawned, this option is ignored.
   */
  fs::FixedPath spawn_executable_pattern_;

  /**
   * @brief String pattern of LD_LIBRARY_PATH environment variable to spawn SOC engines
   * in each NUMA node.
   * @details
   * Similar to spawn_executable_pattern_.
   * The default value is empty, which means we don't overwrite LD_LIBRARY_PATH of this master
   * process. To overwrite master process's LD_LIBRARY_PATH with empty value, put one space etc.
   */
  fs::FixedPath spawn_ld_library_path_pattern_;

  /** converts spawn_executable_pattern_ into a string with the given node ID. */
  std::string   convert_spawn_executable_pattern(int node) const;
  /** converts spawn_ld_library_path_pattern_ into a string with the given node ID. */
  std::string   convert_spawn_ld_library_path_pattern(int node) const;

  EXTERNALIZABLE(SocOptions);
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_OPTIONS_HPP_
