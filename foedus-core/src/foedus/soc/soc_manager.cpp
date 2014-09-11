/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/soc_manager.hpp"
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

}  // namespace soc
}  // namespace foedus
