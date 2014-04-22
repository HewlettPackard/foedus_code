/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/storage_options.hpp>
namespace foedus {
namespace storage {
StorageOptions::StorageOptions() {
}
ErrorStack StorageOptions::load(tinyxml2::XMLElement* /*element*/) {
    return RET_OK;
}
ErrorStack StorageOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for storage manager."));
    return RET_OK;
}
}  // namespace storage
}  // namespace foedus
