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

  /**
   * Launch emulated children as threads.
   */
  void        launch_emulated_children();
  /**
   * Main routine of emulated SOCs.
   */
  static void emulated_child_main(Engine* master_engine, SocId node);

  Engine* const           engine_;
  SharedMemoryRepo        memory_repo_;

  /**
   * Threads that emulate child SOCs.
   * Used when this is a master engine and also children are emulated.
   */
  std::vector<std::thread> child_emulated_threads_;

  /**
   * Process IDs of child SOCs.
   * Used when this is a master engine and also children are not emulated.
   */
  Upid                    child_upids_[kMaxSocs];
};
}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_MANAGER_PIMPL_HPP_
