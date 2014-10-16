/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/soc_manager.hpp"

#include <vector>

#include "foedus/soc/soc_manager_pimpl.hpp"

namespace foedus {
namespace soc {
SocManager::SocManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new SocManagerPimpl(engine);
}
SocManager::~SocManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  SocManager::initialize() { return pimpl_->initialize(); }
bool        SocManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  SocManager::uninitialize() { return pimpl_->uninitialize(); }

SharedMemoryRepo* SocManager::get_shared_memory_repo() { return &pimpl_->memory_repo_; }

void SocManager::trap_spawned_soc_main() {
  std::vector< proc::ProcAndName > procedures;
  trap_spawned_soc_main(procedures);
}

void SocManager::trap_spawned_soc_main(const std::vector< proc::ProcAndName >& procedures) {
  SocManagerPimpl::spawned_child_main(procedures);
}

ErrorStack SocManager::wait_for_children_module(bool init, ModuleType module) {
  return pimpl_->wait_for_children_module(init, module);
}

ErrorStack SocManager::wait_for_master_module(bool init, ModuleType module) {
  return pimpl_->wait_for_master_module(init, module);
}
void SocManager::report_engine_fatal_error() { pimpl_->report_engine_fatal_error(); }

}  // namespace soc
}  // namespace foedus
