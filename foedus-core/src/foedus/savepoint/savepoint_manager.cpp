/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/savepoint/savepoint_manager_pimpl.hpp"
namespace foedus {
namespace savepoint {
SavepointManager::SavepointManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new SavepointManagerPimpl(engine);
}
SavepointManager::~SavepointManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  SavepointManager::initialize() { return pimpl_->initialize(); }
bool        SavepointManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  SavepointManager::uninitialize() { return pimpl_->uninitialize(); }

Epoch SavepointManager::get_initial_current_epoch() const {
  return pimpl_->get_initial_current_epoch();
}
Epoch SavepointManager::get_initial_durable_epoch() const {
  return pimpl_->get_initial_durable_epoch();
}
Epoch SavepointManager::get_saved_durable_epoch() const {
  return pimpl_->get_saved_durable_epoch();
}

ErrorStack SavepointManager::take_savepoint(Epoch new_global_durable_epoch) {
  return pimpl_->take_savepoint(new_global_durable_epoch);
}
LoggerSavepointInfo SavepointManager::get_logger_savepoint(log::LoggerId logger_id) {
  return pimpl_->get_logger_savepoint(logger_id);
}


}  // namespace savepoint
}  // namespace foedus
