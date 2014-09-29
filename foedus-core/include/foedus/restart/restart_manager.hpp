/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_RESTART_RESTART_MANAGER_HPP_
#define FOEDUS_RESTART_RESTART_MANAGER_HPP_
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/restart/fwd.hpp"
namespace foedus {
namespace restart {
/**
 * @brief Restart manager, which recovers the state of database by invoking log-gleaner
 * at start-up.
 * @ingroup RESTART
 * @details
 * @par Simplistic Restart
 * A DBMS usually has a complex ('sophisticated') restart routine especially if they try to reduce
 * start-up time. On the other hand, FOEDUS simply invokes the log gleaner just like a periodic
 * snapshot. This is a huge saving in code complexity. As far as we optimize the log gleaner,
 * the start-up time is not slower than typical replay-every-log-style traditional restart, either.
 */
class RestartManager CXX11_FINAL : public virtual Initializable {
 public:
  explicit RestartManager(Engine* engine);
  ~RestartManager();

  // Disable default constructors
  RestartManager() CXX11_FUNC_DELETE;
  RestartManager(const RestartManager&) CXX11_FUNC_DELETE;
  RestartManager& operator=(const RestartManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

 private:
  RestartManagerPimpl *pimpl_;
};
}  // namespace restart
}  // namespace foedus
#endif  // FOEDUS_RESTART_RESTART_MANAGER_HPP_
