/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_FS_DEVICE_EMULATION_OPTIONS_HPP_
#define FOEDUS_FS_DEVICE_EMULATION_OPTIONS_HPP_
#include <stdint.h>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace fs {
/**
 * @brief Set of configurations to emulate slower devices for some experiments.
 * @ingroup FILESYSTEM
 * @details
 * For snapshot files and log files, we have an option to emulate slower devices.
 * In some experiments, we use them to emulate NVM/SSD/etc on DRAM (RAMDisk).
 * This is a POD.
 */
struct DeviceEmulationOptions CXX11_FINAL : public virtual externalize::Externalizable {
  DeviceEmulationOptions() {
    disable_direct_io_ = false;
    emulated_seek_latency_cycles_ = 0;
    emulated_read_kb_cycles_ = 0;
    emulated_write_kb_cycles_ = 0;
  }

  /** [Experiments] Whether to disable Direct I/O and use non-direct I/O instead. */
  bool        disable_direct_io_;

  /** [Experiments] additional CPU cycles to busy-wait for each seek. 0 (default) disables it. */
  uint32_t    emulated_seek_latency_cycles_;

  /**
   * [Experiments] additional CPU cycles to busy-wait for each 1KB read. 0 (default) disables it.
   * For example, 4kb read is seek+read*4 cycles, 1MB read is seek+read*1000 cycles.
   */
  uint32_t    emulated_read_kb_cycles_;

  /**
   * [Experiments] additional CPU cycles to busy-wait for each 1KB write. 0 (default) disables it.
   */
  uint32_t    emulated_write_kb_cycles_;

  EXTERNALIZABLE(DeviceEmulationOptions);
};
}  // namespace fs
}  // namespace foedus
#endif  // FOEDUS_FS_DEVICE_EMULATION_OPTIONS_HPP_
