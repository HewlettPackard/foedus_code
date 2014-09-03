/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage_options.hpp"
namespace foedus {
namespace storage {
StorageOptions::StorageOptions() {
  max_storages_ = kDefaultMaxStorages;
}
ErrorStack StorageOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, max_storages_);
  return kRetOk;
}
ErrorStack StorageOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for storage manager."));
  EXTERNALIZE_SAVE_ELEMENT(element, max_storages_,
    "Maximum number of storages in this database.");
  return kRetOk;
}
}  // namespace storage
}  // namespace foedus
