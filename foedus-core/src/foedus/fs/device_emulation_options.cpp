/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/fs/device_emulation_options.hpp>
namespace foedus {
namespace fs {
ErrorStack DeviceEmulationOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, disable_direct_io_);
    EXTERNALIZE_LOAD_ELEMENT(element, emulated_seek_latency_ns_);
    EXTERNALIZE_LOAD_ELEMENT(element, emulated_scan_latency_ns_);
    return kRetOk;
}

ErrorStack DeviceEmulationOptions::save(tinyxml2::XMLElement* element) const {
    EXTERNALIZE_SAVE_ELEMENT(element, disable_direct_io_,
        "Whether to disable Direct I/O and use non-direct I/O instead.");
    EXTERNALIZE_SAVE_ELEMENT(element, emulated_seek_latency_ns_,
        "additional nanosec to busy-wait for each seek. 0 (default) disables it.");
    EXTERNALIZE_SAVE_ELEMENT(element, emulated_scan_latency_ns_,
        "additional nanosec to busy-wait for each 1KB read. 0 (default) disables it.");
    return kRetOk;
}

}  // namespace fs
}  // namespace foedus
