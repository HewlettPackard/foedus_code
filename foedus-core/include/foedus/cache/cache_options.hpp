/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CACHE_CACHE_OPTIONS_HPP_
#define FOEDUS_CACHE_CACHE_OPTIONS_HPP_
#include <iosfwd>
namespace foedus {
namespace cache {
/**
 * @brief Set of options for snapshot cache manager.
 * @ingroup CACHE
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct CacheOptions {
    /**
     * Constructs option values with default values.
     */
    CacheOptions();

    friend std::ostream& operator<<(std::ostream& o, const CacheOptions& v);
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_OPTIONS_HPP_
