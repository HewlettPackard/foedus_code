/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/log_manager.hpp>
#include <foedus/log/log_manager_pimpl.hpp>
namespace foedus {
namespace log {
LogManager::LogManager(Engine* engine) {
    pimpl_ = new LogManagerPimpl(engine);
}
LogManager::~LogManager() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack  LogManager::initialize() { return pimpl_->initialize(); }
bool        LogManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  LogManager::uninitialize() { return pimpl_->uninitialize(); }

}  // namespace log
}  // namespace foedus
