/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  /** Main routine of emulated SOCs. */
  void        emulated_child_main(SocId node);
  /** Main routine of forked SOCs. @return exit code as a process */
  int         forked_child_main(SocId node);
  /** Main routine of spawned SOCs. This is invoked from user's main(), so it's static. */
  static void spawned_child_main(const std::vector< proc::ProcAndName >& procedures);

  static ErrorStack child_main_common(
    EngineType engine_type,
    Upid master_upid,
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
