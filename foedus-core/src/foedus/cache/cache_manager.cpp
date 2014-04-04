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
ErrorStack CacheManager::initialize() {
    if (initialized_) {
        return ERROR_STACK(ERROR_CODE_ALREADY_INITIALIZED);
    }
    // Acquire all memories
    initialized_ = true;
    return RET_OK;
}

ErrorStack CacheManager::uninitialize() {
    if (!initialized_) {
        return RET_OK;
    }
    // Release all memories
    initialized_ = false;
    return RET_OK;
}
}  // namespace cache
}  // namespace foedus
