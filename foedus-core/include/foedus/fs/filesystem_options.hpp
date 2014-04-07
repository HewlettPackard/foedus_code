/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_FS_FILESYSTEM_OPTIONS_HPP_
#define FOEDUS_FS_FILESYSTEM_OPTIONS_HPP_
#include <iosfwd>
namespace foedus {
namespace fs {
/**
 * @brief Set of options for filesystem wrapper.
 * @ingroup FILESYSTEM
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct FilesystemOptions {
    /**
     * Constructs option values with default values.
     */
    FilesystemOptions() {}

    friend std::ostream& operator<<(std::ostream& o, const FilesystemOptions& v);
};
}  // namespace fs
}  // namespace foedus
#endif  // FOEDUS_FS_FILESYSTEM_OPTIONS_HPP_
