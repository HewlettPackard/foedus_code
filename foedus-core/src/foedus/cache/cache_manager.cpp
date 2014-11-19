/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/cache/cache_manager.hpp"

#include <string>

#include "foedus/cache/cache_manager_pimpl.hpp"

namespace foedus {
namespace cache {

CacheManager::CacheManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new CacheManagerPimpl(engine);
}
CacheManager::~CacheManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack CacheManager::initialize_once() { return pimpl_->initialize_once(); }
ErrorStack CacheManager::uninitialize_once() { return pimpl_->uninitialize_once(); }
std::string CacheManager::describe() const { return pimpl_->describe(); }

}  // namespace cache
}  // namespace foedus
