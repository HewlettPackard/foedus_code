/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <foedus/fs/device_emulation_options.hpp>
#include <string>
#include <vector>
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
   * Two special placeholders can be used; $NODE$ and $PARTITION$.
   * $NODE$ is replaced with the NUMA node number.
   * $PARTITION$ is replaced with the partition in the node (0 to partitions_per_node_ - 1).
   * For example,
   * \li "/data/node_$NODE$/part_$PARTITION$" becomes "/data/node_1/part_0" on node-1 and part-0.
   * \li "/data/folder_$INDEX$" becomes "/data/folder_1" on any node and partition-1.
   *
   * Both are optional. You can specify a fixed path without the patterns, which means you will
   * use the same folder for multiple partitions and nodes.
   * Even in that case, snapshot file names include uniquefiers, so it wouldn't cause any data
   * corruption. It just makes things harder for poor sysadmins.
   *
   * The snapshot folders are also the granularity of partitioning.
   * Each snapshot phase starts with partitioning of logs using random samples, then
   * scatter-gather log entries to assigned partitions like Map-Reduce.
   *
   * The default value is "snapshots/node_$NODE$/partition_$PARTITION$".
   */
  std::string                         folder_path_pattern_;

  /**
   * @brief Number of snapshot folders (ie partitions) per NUMA node.
   * @details
   * This value must be at least 1 (which is also default).
   * A larger value might be able to employ more CPU power during snapshot construction,
   * but makes the scatter-gather more fine grained, potentially making it slower.
   */
  uint16_t                            partitions_per_node_;

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

  /** converts folder_path_pattern_ into a string with the given IDs. */
  std::string     convert_folder_path_pattern(int node, int partition) const;

  /**
   * Returns the path of first node's first partition, which is also used as the primary place
   * to write out global files, such as snapshot metadata.
   */
  std::string     get_primary_folder_path() const {
    return convert_folder_path_pattern(0, 0);
  }

  EXTERNALIZABLE(SnapshotOptions);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
