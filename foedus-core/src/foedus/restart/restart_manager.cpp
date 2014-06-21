/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/restart/restart_manager.hpp"
#include "foedus/restart/restart_manager_pimpl.hpp"
namespace foedus {
namespace restart {
RestartManager::RestartManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new RestartManagerPimpl(engine);
}
RestartManager::~RestartManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  RestartManager::initialize() { return pimpl_->initialize(); }
bool        RestartManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  RestartManager::uninitialize() { return pimpl_->uninitialize(); }

}  // namespace restart
}  // namespace foedus
