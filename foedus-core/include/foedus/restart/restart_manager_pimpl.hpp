/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_
#define FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/restart/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

namespace foedus {
namespace restart {
/** Shared data in RestartManagerPimpl. */
struct RestartManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  RestartManagerControlBlock() = delete;
  ~RestartManagerControlBlock() = delete;

  // nothing so far
};

/**
 * @brief Pimpl object of RestartManager.
 * @ingroup RESTART
 * @details
 * A private pimpl object for RestartManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class RestartManagerPimpl final : public DefaultInitializable {
 public:
  RestartManagerPimpl() = delete;
  explicit RestartManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  /**
   * Recover the state of database from recent snapshot files and transaction logs.
   * @pre is_initialized(), and all other modules in the engine is_initialized().
   */
  ErrorStack  recover();
  /**
   * Redo metadata operation (create/drop storage) since the latest snapshot.
   * Essentially this is the only thing the restart manager has to do.
   */
  ErrorStack  redo_meta_logs(Epoch durable_epoch, Epoch snapshot_epoch);

  Engine* const           engine_;
  RestartManagerControlBlock* control_block_;
};

static_assert(
  sizeof(RestartManagerControlBlock) <= soc::GlobalMemoryAnchors::kRestartManagerMemorySize,
  "RestartManagerControlBlock is too large.");
}  // namespace restart
}  // namespace foedus
#endif  // FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_
