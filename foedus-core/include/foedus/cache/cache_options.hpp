/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_OPTIONS_HPP_
#define FOEDUS_CACHE_CACHE_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
namespace foedus {
namespace cache {
/**
 * @brief Set of options for snapshot cache manager.
 * @ingroup CACHE
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct CacheOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /**
   * Constructs option values with default values.
   */
  CacheOptions();

  EXTERNALIZABLE(CacheOptions);
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_OPTIONS_HPP_
