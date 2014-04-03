/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_FS_FILESYSTEM_OPTIONS_HPP_
#define FOEDUS_FS_FILESYSTEM_OPTIONS_HPP_
#include <cstdint>
#include <iosfwd>
namespace foedus {
namespace fs {
/**
 * @brief Set of options for log manager.
 * @ingroup FILESYSTEM
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct FilesystemOptions {
    /**
     * Constructs option values with default values.
     */
    FilesystemOptions();

    /** Whether to disable Direct I/O and use non-direct I/O instead. */
    bool                        disable_direct_io_;

    /** [Experiments] additional nanosec to busy-wait for each seek. 0 (default) disables it. */
    uint32_t                    emulated_seek_latency_ns_;

    /** [Experiments] additional nanosec to busy-wait for each 1KB read. 0 (default) disables it. */
    uint32_t                    emulated_scan_latency_ns_;
};
}  // namespace fs
}  // namespace foedus
std::ostream& operator<<(std::ostream& o, const foedus::fs::FilesystemOptions& v);
#endif  // FOEDUS_FS_FILESYSTEM_OPTIONS_HPP_
