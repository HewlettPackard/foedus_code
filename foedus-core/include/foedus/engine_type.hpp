/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_TYPE_HPP_
#define FOEDUS_ENGINE_TYPE_HPP_
namespace foedus {
/**
 * @brief Type of an engine instance of how to launch it.
 * @ingroup ENGINE
 */
enum EngineType {
  /**
   * @brief The central instance that launches child engines on each NUMA node (SOC).
   * @details
   * This is the only engine type a user explicitly instantiates.
   */
  kMaster,

  /**
   * @brief A child SOC instance launched just as a thread in the same process as master.
   * @details
   * This is the most handy way of launching child SOCs, but has a scalability limit
   * around 30-50 threads in one process. Instead, users can easily specify function pointers
   * and also debug/profile the program just like a usual standalone program.
   * Most testing and development uses this.
   */
  kChildEmulated,

  /**
   * @brief A child SOC instance launched via fork().
   * @details
   * This is also handy next to emulated children, and also does not require a change
   * in main() function of the program as the forked processes start from the fork() position
   * rather than main().
   * fork() preserves address space \b as-of the fork, so if the user pre-registers function
   * pointers \b BEFORE the fork (Engine's initialize()), they can be also available in all SOCs.
   * However, after the fork, the only way to register user-specified procedures
   * is to provide additional shared libraries.
   * One (probably) minor drawback: fork() is not available in Windows.
   */
  kChildForked,

  /**
   * @brief A child SOC instance launched via spawn().
   * @details
   * In order to use this type, the user has to put one-liner in her main() function to
   * catch the spawned child process and forward the control to FOEDUS library.
   * Instead, the user can register arbitrary function pointers in the main() function, which
   * is executed in all SOCs. This way of registering user procedures do not need to compile
   * separate shared libraries.
   * Further, the user can choose to launch different binaries for each SOC, as an alternative of
   * executable/library text replication for each NUMA node to reduce instruction cache miss cost.
   */
  kChildLocalSpawned,

  /**
   * @brief A child SOC instance launched in other machines.
   * @details
   * This type is not implemented yet.
   * On cache-incoherent machines, we have to use this type.
   * @todo How to launch programs remotely? How to use RVMA?
   */
  kChildRemoteSpawned,
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_TYPE_HPP_
