/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_MANAGER_HPP_
#define FOEDUS_CACHE_CACHE_MANAGER_HPP_
#include <foedus/initializable.hpp>
#include <foedus/cache/cache_options.hpp>
namespace foedus {
namespace cache {
/**
 * @brief Brief description of this class.
 * @details
 * Detailed description of this class.
 */
class CacheManager : public virtual Initializable {
 public:
    explicit CacheManager(const CacheOptions &options);
    ~CacheManager();

    // Disable default constructors
    CacheManager() CXX11_FUNC_DELETE;
    CacheManager(const CacheManager &) CXX11_FUNC_DELETE;
    CacheManager& operator=(const CacheManager &) CXX11_FUNC_DELETE;

    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE { return initialized_; }
    ErrorStack  uninitialize() CXX11_OVERRIDE;

    const CacheOptions&    get_options() const { return options_; }

 private:
    CacheOptions    options_;
    bool            initialized_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_MANAGER_HPP_
