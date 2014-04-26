/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_manager.hpp>
#include <foedus/xct/xct_manager_pimpl.hpp>
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

Epoch       XctManager::get_global_epoch(bool fence_before_get) const {
    return pimpl_->get_global_epoch(fence_before_get);
}
ErrorStack  XctManager::begin_xct(thread::Thread* context)  { return pimpl_->begin_xct(context); }
ErrorStack  XctManager::prepare_commit_xct(thread::Thread* context, Epoch *commit_epoch) {
    return pimpl_->prepare_commit_xct(context, commit_epoch);
}
ErrorStack  XctManager::abort_xct(thread::Thread* context)  { return pimpl_->abort_xct(context); }

}  // namespace xct
}  // namespace foedus
