/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/device_emulation_options.hpp>
#include <ostream>

std::ostream& operator<<(std::ostream& o, const foedus::fs::DeviceEmulationOptions& v) {
    o << "  disable_direct_io_=" << v.disable_direct_io_ << std::endl;
    o << "  emulated_seek_latency_ns_=" << v.emulated_seek_latency_ns_ << std::endl;
    o << "  emulated_scan_latency_ns_=" << v.emulated_scan_latency_ns_ << std::endl;
    return o;
}
