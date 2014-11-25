/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/xct/xct_manager.hpp"
#include "foedus/xct/xct_manager_pimpl.hpp"
namespace foedus {
namespace xct {
XctManager::XctManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new XctManagerPimpl(engine);
}
XctManager::~XctManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  XctManager::initialize() { return pimpl_->initialize(); }
bool        XctManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  XctManager::uninitialize() { return pimpl_->uninitialize(); }
void        XctManager::pause_accepting_xct() { pimpl_->pause_accepting_xct(); }
void        XctManager::resume_accepting_xct() { pimpl_->resume_accepting_xct(); }
void        XctManager::wait_for_current_global_epoch(Epoch target_epoch) {
  pimpl_->wait_for_current_global_epoch(target_epoch);
}


}  // namespace xct
}  // namespace foedus
