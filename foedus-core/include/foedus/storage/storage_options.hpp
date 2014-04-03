/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
#define FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
#include <iosfwd>
namespace foedus {
namespace storage {
/**
 * @brief Set of options for storage manager.
 * @ingroup STORAGE
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct StorageOptions {
    /**
     * Constructs option values with default values.
     */
    StorageOptions();
};
}  // namespace storage
}  // namespace foedus
std::ostream& operator<<(std::ostream& o, const foedus::storage::StorageOptions& v);
#endif  // FOEDUS_STORAGE_STORAGE_OPTIONS_HPP_
