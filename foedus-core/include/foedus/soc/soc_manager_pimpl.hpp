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
#ifndef FOEDUS_SOC_SOC_MANAGER_PIMPL_HPP_
#define FOEDUS_SOC_SOC_MANAGER_PIMPL_HPP_

#include <unistd.h>
#include <sys/types.h>

#include <thread>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/soc/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_id.hpp"

namespace foedus {
namespace soc {
/**
 * @brief Pimpl object of SocManager.
 * @ingroup SOC
 * @details
 * A private pimpl object for SocManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SocManagerPimpl final : public DefaultInitializable {
 public:
  SocManagerPimpl() = delete;
  explicit SocManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  void        report_engine_fatal_error();

  /** Called as part of initialize_once() if this is a master engine */
  ErrorStack  initialize_master();

  /** Called as part of initialize_once() if this is a child SOC engine */
  ErrorStack  initialize_child();

  /** Launch emulated children as threads. */
  ErrorStack  launch_emulated_children();
  /** Launch children via fork. */
  ErrorStack  launch_forked_children();
  /** Launch children via spawn. */
  ErrorStack  launch_spawned_children();
  /** Wait for child SOCs to start up and at least finish attaching shared memory. */
  ErrorStack  wait_for_child_attach();
  /** Wait for child SOCs to terminate. */
  ErrorStack  wait_for_child_terminate();
  /** Wait for master engine to finish upto the specified status. */
  ErrorStack  wait_for_master_status(MasterEngineStatus::StatusCode target_status);
  ErrorStack  wait_for_master_module(bool init, ModuleType module);
  ErrorStack  wait_for_children_module(bool init, ModuleType module);
  /** Main routine of emulated SOCs. */
  void        emulated_child_main(SocId node);
  /** Main routine of forked SOCs. @return exit code as a process */
  int         forked_child_main(SocId node);
  /** Main routine of spawned SOCs. This is invoked from user's main(), so it's static. */
  static void spawned_child_main(const std::vector< proc::ProcAndName >& procedures);

  static ErrorStack child_main_common(
    EngineType engine_type,
    Upid master_upid,
    Eid master_eid,
    SocId node,
    const std::vector< proc::ProcAndName >& procedures);

  Engine* const           engine_;
  SharedMemoryRepo        memory_repo_;

  /**
   * Threads that emulate child SOCs.
   * Used when this is a master engine and also children are emulated.
   */
  std::vector<std::thread> child_emulated_threads_;
  /** And their engines. These pointers become invalid when the threads quit */
  std::vector< Engine* >  child_emulated_engines_;

  /**
   * Process IDs of child SOCs.
   * Used when this is a master engine and also children are not emulated.
   */
  Upid                    child_upids_[kMaxSocs];
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_MANAGER_PIMPL_HPP_
