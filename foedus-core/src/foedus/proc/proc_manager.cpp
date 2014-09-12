/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/proc/proc_manager.hpp"

#include "foedus/proc/proc_id.hpp"
#include "foedus/proc/proc_manager_pimpl.hpp"

namespace foedus {
namespace proc {
ProcManager::ProcManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new ProcManagerPimpl(engine);
}
ProcManager::~ProcManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  ProcManager::initialize() { return pimpl_->initialize(); }
bool        ProcManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  ProcManager::uninitialize() { return pimpl_->uninitialize(); }

ErrorStack  ProcManager::pre_register(const ProcAndName& proc_and_name) {
  return pimpl_->pre_register(proc_and_name);
}
ErrorStack  ProcManager::local_register(const ProcAndName& proc_and_name) {
  return pimpl_->local_register(proc_and_name);
}
ErrorStack  ProcManager::emulated_register(const ProcAndName& proc_and_name) {
  return pimpl_->emulated_register(proc_and_name);
}

}  // namespace proc
}  // namespace foedus
