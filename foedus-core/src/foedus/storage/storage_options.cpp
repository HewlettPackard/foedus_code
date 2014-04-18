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

std::ostream& operator<<(std::ostream& o, const StorageOptions& /*v*/) {
    o << "  <StorageOptions>" << std::endl;
    o << "  </StorageOptions>" << std::endl;
    return o;
}
}  // namespace storage
}  // namespace foedus
