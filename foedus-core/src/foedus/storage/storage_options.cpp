/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage_options.hpp"
namespace foedus {
namespace storage {
StorageOptions::StorageOptions() {
  max_storages_ = kDefaultMaxStorages;
  partitioner_data_memory_mb_ = kDefaultPartitionerDataMemoryMb;
}
ErrorStack StorageOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, max_storages_);
  EXTERNALIZE_LOAD_ELEMENT(element, partitioner_data_memory_mb_);
  return kRetOk;
}
ErrorStack StorageOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for storage manager."));
  EXTERNALIZE_SAVE_ELEMENT(element, max_storages_,
    "Maximum number of storages in this database.");
  EXTERNALIZE_SAVE_ELEMENT(element, partitioner_data_memory_mb_,
    "Size in MB of a shared memory buffer allocated for all partitioners during log gleaning."
    "Increase this value when you have a large number of storages that have large partitioning"
    " information (eg. long keys).");
  return kRetOk;
}
}  // namespace storage
}  // namespace foedus
