/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/cache/cache_options.hpp>
namespace foedus {
namespace cache {
CacheOptions::CacheOptions() {
}
ErrorStack CacheOptions::load(tinyxml2::XMLElement* /*element*/) {
    return RET_OK;
}
ErrorStack CacheOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for snapshot cache manager."));
    return RET_OK;
}

}  // namespace cache
}  // namespace foedus
