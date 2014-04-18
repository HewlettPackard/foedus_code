/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/fs/device_emulation_options.hpp>
#include <ostream>
namespace foedus {
namespace fs {
std::ostream& operator<<(std::ostream& o, const foedus::fs::DeviceEmulationOptions& v) {
    o << "    <DeviceEmulationOptions>" << std::endl;
    EXTERNALIZE_WRITE(disable_direct_io_);
    EXTERNALIZE_WRITE(emulated_seek_latency_ns_);
    EXTERNALIZE_WRITE(emulated_scan_latency_ns_);
    o << "    </DeviceEmulationOptions>" << std::endl;
    return o;
}
}  // namespace fs
}  // namespace foedus
