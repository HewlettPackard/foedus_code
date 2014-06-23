/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#include <string>
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/fs/device_emulation_options.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief Set of options for snapshot manager.
 * @ingroup SNAPSHOT
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct SnapshotOptions CXX11_FINAL : public virtual externalize::Externalizable {
  enum Constants {
    kDefaultSnapshotTriggerPagePoolPercent = 100,
    kDefaultSnapshotIntervalMilliseconds = 60000,
    kDefaultLogMapperBucketKb           = 1024,
    kDefaultLogMapperIoBufferKb         = 2048,
    kDefaultLogReducerBufferMb           = 256,
  };

  /**
   * Constructs option values with default values.
   */
  SnapshotOptions();

  /**
   * @brief String pattern of path of snapshot folders in each NUMA node.
   * @details
   * This specifies the path of the folders to contain snapshot files in each NUMA node.
   * A special placeholder $NODE$ will be replaced with the NUMA node number.
   * For example, "/data/node_$NODE$" becomes "/data/node_1" on node-1.
   *
   * It is optional. You can specify a fixed path without the patterns, which means you will
   * use the same folder at all nodes.
   * Even in that case, snapshot file names include uniquefiers, so it wouldn't cause any data
   * corruption. It just makes things harder for poor sysadmins.
   *
   * The default value is "snapshots/node_$NODE$".
   */
  std::string                         folder_path_pattern_;

  /**
   * When the main page pool runs under this percent (roughly calculated) of free pages,
   * snapshot manager starts snapshotting to drop volatile pages even before the interval.
   * Default is 100 (no check).
   */
  uint16_t                            snapshot_trigger_page_pool_percent_;

  /**
   * Interval in milliseconds to take snapshots.
   * Default is one minute.
   */
  uint32_t                            snapshot_interval_milliseconds_;

  /**
   * The size in KB of bucket (buffer for each partition) in mapper.
   * The larger, the less freuquently each mapper communicates with reducers.
   * 1024 (1MB) should be a good number.
   */
  uint32_t                            log_mapper_bucket_kb_;

  /**
   * The size in KB of IO buffer to read log files in mapper.
   * 1024 (1MB) should be a good number.
   */
  uint32_t                            log_mapper_io_buffer_kb_;

  /**
   * The size in MB of a buffer to store log entries in reducer (partition).
   * Each reducer receives log entries from all mappers, so the right size is likely much
   * larger than log_mapper_bucket_kb_.
   *
   * Reducer sorts and dumps out this buffer to a file, then does merge-sort at the end.
   * If this buffer can contain all the logs while snapshotting, it will not do any I/O
   * thus be significanltly faster.
   * If you have a big DRAM, you might want to specify a large number for that reason.
   */
  uint32_t                            log_reducer_buffer_mb_;

  /** Settings to emulate slower data device. */
  foedus::fs::DeviceEmulationOptions  emulation_;

  /** converts folder_path_pattern_ into a string with the given node. */
  std::string     convert_folder_path_pattern(int node) const;

  /**
   * Returns the path of first node, which is also used as the primary place
   * to write out global files, such as snapshot metadata.
   */
  std::string     get_primary_folder_path() const {
    return convert_folder_path_pattern(0);
  }

  EXTERNALIZABLE(SnapshotOptions);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
