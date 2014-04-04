/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/filesystem_options.hpp>
#include <ostream>
std::ostream& operator<<(std::ostream& o, const foedus::fs::FilesystemOptions& /*v*/) {
    o << "Filesystem options:" << std::endl;
    return o;
}
