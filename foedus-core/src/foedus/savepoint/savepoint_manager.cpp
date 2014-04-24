/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/savepoint/savepoint_manager.hpp>
#include <foedus/savepoint/savepoint_manager_pimpl.hpp>
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

const Savepoint& SavepointManager::get_savepoint_fast() const {
    return pimpl_->get_savepoint_fast();
}
Savepoint SavepointManager::get_savepoint_safe() const { return pimpl_->get_savepoint_safe(); }

}  // namespace savepoint
}  // namespace foedus
