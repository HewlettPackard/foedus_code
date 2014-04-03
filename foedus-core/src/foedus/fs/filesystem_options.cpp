/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/filesystem_options.hpp>
#include <ostream>
namespace foedus {
namespace fs {
FilesystemOptions::FilesystemOptions() {
    disable_direct_io_ = false;
    emulated_seek_latency_ns_ = 0;
    emulated_scan_latency_ns_ = 0;
}

}  // namespace fs
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::fs::FilesystemOptions& v) {
    o << "Filesystem options:" << std::endl;
    o << "  disable_direct_io_=" << v.disable_direct_io_ << std::endl;
    o << "  emulated_seek_latency_ns_=" << v.emulated_seek_latency_ns_ << std::endl;
    o << "  emulated_scan_latency_ns_=" << v.emulated_scan_latency_ns_ << std::endl;
    return o;
}
