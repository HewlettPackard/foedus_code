/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_MANAGER_HPP_
#define FOEDUS_CACHE_CACHE_MANAGER_HPP_

#include <string>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/cache/cache_options.hpp"
#include "foedus/cache/fwd.hpp"

namespace foedus {
namespace cache {
/**
 * @brief Snapshot cache manager.
 * @details
 * A snapshot cache manager exists in each SOC engine, maintaining their SOC-local
 * snapshot cache. Everything is SOC-local, so actually a master engine does nothing here.
 */
class CacheManager : public DefaultInitializable {
 public:
  CacheManager() CXX11_FUNC_DELETE;
  explicit CacheManager(Engine* engine);
  ~CacheManager();
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  const CacheOptions&    get_options() const;

  /** kind of to_string(). this might be slow, so do not call too often */
  std::string describe() const;

 private:
  CacheManagerPimpl* pimpl_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_MANAGER_HPP_
