/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_MANAGER_HPP_
#define FOEDUS_CACHE_CACHE_MANAGER_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/cache/cache_options.hpp>
namespace foedus {
namespace cache {
/**
 * @brief Brief description of this class.
 * @details
 * Detailed description of this class.
 */
class CacheManager : public DefaultInitializable {
 public:
  CacheManager() CXX11_FUNC_DELETE;
  explicit CacheManager(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  const CacheOptions&    get_options() const;

 private:
  Engine* const           engine_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_MANAGER_HPP_
