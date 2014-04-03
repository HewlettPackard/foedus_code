/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/storage_options.hpp>
#include <ostream>
namespace foedus {
namespace storage {
StorageOptions::StorageOptions() {
}
}  // namespace storage
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::storage::StorageOptions& /*v*/) {
    o << "Storage options:" << std::endl;
    return o;
}
