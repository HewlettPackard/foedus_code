/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/cache/cache_options.hpp>
#include <ostream>
namespace foedus {
namespace cache {
CacheOptions::CacheOptions() {
}
}  // namespace cache
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::cache::CacheOptions& /*v*/) {
    o << "Snapshot Cache options:" << std::endl;
    return o;
}
