/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_ENGINE_TYPE_HPP_
#define FOEDUS_ENGINE_TYPE_HPP_

#include <stdint.h>

namespace foedus {

/**
 * An Engine ID to differentiate two Engine objects instantiated in the same process.
 * This and soc::Upid uniquely identify an Engine instance.
 */
typedef uint64_t Eid;

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
