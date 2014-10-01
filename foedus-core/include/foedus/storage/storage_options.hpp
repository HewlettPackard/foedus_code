/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
#define FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"
namespace foedus {
namespace storage {
/**
 * @brief Set of options for storage manager.
 * @ingroup STORAGE
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct StorageOptions CXX11_FINAL : public virtual externalize::Externalizable {
  enum Constants {
    kDefaultMaxStorages = 1 << 9,
    kDefaultPartitionerDataMemoryMb = 1,
  };
  /**
   * Constructs option values with default values.
   */
  StorageOptions();

  /**
   * Maximum number of storages in this database.
   */
  uint32_t                max_storages_;

  /**
   * Size in MB of a shared memory buffer allocated for all partitioners during log gleaning.
   * Increase this value when you have a large number of storages that have large partitioning
   * information (eg. long keys).
   */
  uint32_t                partitioner_data_memory_mb_;

  EXTERNALIZABLE(StorageOptions);
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
