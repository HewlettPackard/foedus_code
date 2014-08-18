/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/externalize/externalizable.hpp"
#include "foedus/fs/device_emulation_options.hpp"
namespace foedus {
namespace fs {
ErrorStack DeviceEmulationOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, disable_direct_io_);
  EXTERNALIZE_LOAD_ELEMENT(element, null_device_);
  EXTERNALIZE_LOAD_ELEMENT(element, emulated_seek_latency_cycles_);
  EXTERNALIZE_LOAD_ELEMENT(element, emulated_read_kb_cycles_);
  EXTERNALIZE_LOAD_ELEMENT(element, emulated_write_kb_cycles_);
  return kRetOk;
}

ErrorStack DeviceEmulationOptions::save(tinyxml2::XMLElement* element) const {
  EXTERNALIZE_SAVE_ELEMENT(element, disable_direct_io_,
    "Whether to disable Direct I/O and use non-direct I/O instead.");
  EXTERNALIZE_SAVE_ELEMENT(element, null_device_,
    "As if we write out to /dev/null. Used to measure performance w/o I/O.");
  EXTERNALIZE_SAVE_ELEMENT(element, emulated_seek_latency_cycles_,
    "additional CPU cycles to busy-wait for each seek. 0 (default) disables it.");
  EXTERNALIZE_SAVE_ELEMENT(element, emulated_read_kb_cycles_,
    "additional CPU cycles to busy-wait for each 1KB read. 0 (default) disables it."
    " For example, 4kb read is seek+scan*4 cycles, 1MB read is seek+scan*1000 cycles.");
  EXTERNALIZE_SAVE_ELEMENT(element, emulated_write_kb_cycles_,
    "additional CPU cycles to busy-wait for each 1KB write. 0 (default) disables it.");
  return kRetOk;
}

}  // namespace fs
}  // namespace foedus
