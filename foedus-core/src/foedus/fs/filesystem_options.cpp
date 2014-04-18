/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/filesystem_options.hpp>
#include <ostream>
namespace foedus {
namespace fs {
std::ostream& operator<<(std::ostream& o, const FilesystemOptions& /*v*/) {
    o << "  <FilesystemOptions>" << std::endl;
    o << "  </FilesystemOptions>" << std::endl;
    return o;
}
}  // namespace fs
}  // namespace foedus
