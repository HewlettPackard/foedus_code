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

std::ostream& operator<<(std::ostream& o, const CacheOptions& /*v*/) {
    o << "  <SnapshotCacheOptions>" << std::endl;
    o << "  </SnapshotCacheOptions>" << std::endl;
    return o;
}

}  // namespace cache
}  // namespace foedus
