/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_
#define FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/restart/fwd.hpp>
namespace foedus {
namespace restart {
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

  Engine* const           engine_;
};
}  // namespace restart
}  // namespace foedus
#endif  // FOEDUS_RESTART_RESTART_MANAGER_PIMPL_HPP_
