/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/cache/cache_manager.hpp>
namespace foedus {
namespace cache {
CacheManager::CacheManager(const CacheOptions &options) : options_(options), initialized_(false) {
}
CacheManager::~CacheManager() {
}
}  // namespace cache
}  // namespace foedus
