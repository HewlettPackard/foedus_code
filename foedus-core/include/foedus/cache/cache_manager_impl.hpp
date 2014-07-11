/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_MANAGER_PIMPL_HPP_
#define FOEDUS_CACHE_CACHE_MANAGER_PIMPL_HPP_

#include <mutex>
#include <thread>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/memory/fwd.hpp"

namespace foedus {
namespace cache {
/**
 * @brief Pimpl object of CacheManager.
 * @ingroup CACHE
 * @details
 * A private pimpl object for CacheManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class CacheManagerPimpl final : public DefaultInitializable {
 public:
  CacheManagerPimpl() = delete;
  explicit CacheManagerPimpl(Engine* engine);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  Engine* const                   engine_;


  std::vector<std::thread>        cleaners_;
  std::vector<memory::PagePool*>  pools_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_MANAGER_PIMPL_HPP_
