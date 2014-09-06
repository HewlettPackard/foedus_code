/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot_options.hpp"

#include <string>

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace snapshot {
SnapshotOptions::SnapshotOptions() {
  folder_path_pattern_ = "snapshots/node_$NODE$";
  snapshot_trigger_page_pool_percent_ = kDefaultSnapshotTriggerPagePoolPercent;
  snapshot_interval_milliseconds_ = kDefaultSnapshotIntervalMilliseconds;
  log_mapper_bucket_kb_ = kDefaultLogMapperBucketKb;
  log_mapper_io_buffer_mb_ = kDefaultLogMapperIoBufferMb;
  log_reducer_buffer_mb_ = kDefaultLogReducerBufferMb;
  log_reducer_dump_io_buffer_mb_ = kDefaultLogReducerDumpIoBufferMb;
  log_reducer_read_io_buffer_kb_ = kDefaultLogReducerReadIoBufferKb;
  snapshot_writer_page_pool_size_mb_ = kDefaultSnapshotWriterPagePoolSizeMb;
  snapshot_writer_intermediate_pool_size_mb_ = kDefaultSnapshotWriterIntermediatePoolSizeMb;
}

std::string SnapshotOptions::convert_folder_path_pattern(int node) const {
  return assorted::replace_all(folder_path_pattern_.str(), "$NODE$", node);
}

std::string SnapshotOptions::construct_snapshot_file_path(int snapshot_id, int node) const {
  return convert_folder_path_pattern(node)
    + std::string("/snapshot_")
    + std::to_string(snapshot_id)
    + std::string("_")
    + std::to_string(node);
}


ErrorStack SnapshotOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, folder_path_pattern_);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_trigger_page_pool_percent_);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_interval_milliseconds_);
  EXTERNALIZE_LOAD_ELEMENT(element, log_mapper_bucket_kb_);
  EXTERNALIZE_LOAD_ELEMENT(element, log_mapper_io_buffer_mb_);
  EXTERNALIZE_LOAD_ELEMENT(element, log_reducer_buffer_mb_);
  EXTERNALIZE_LOAD_ELEMENT(element, log_reducer_dump_io_buffer_mb_);
  EXTERNALIZE_LOAD_ELEMENT(element, log_reducer_read_io_buffer_kb_);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_writer_page_pool_size_mb_);
  EXTERNALIZE_LOAD_ELEMENT(element, snapshot_writer_intermediate_pool_size_mb_);
  CHECK_ERROR(get_child_element(element, "SnapshotDeviceEmulationOptions", &emulation_))
  return kRetOk;
}

ErrorStack SnapshotOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for snapshot manager"));

  EXTERNALIZE_SAVE_ELEMENT(element, folder_path_pattern_,
    "String pattern of path of snapshot folders in each NUMA node.\n"
    "This specifies the path of the folders to contain snapshot files in each NUMA node.\n"
    " A special placeholder $NODE$ will be replaced with the NUMA node number."
    " For example, /data/node_$NODE$ becomes /data/node_1 on node-1.");
  EXTERNALIZE_SAVE_ELEMENT(element, snapshot_trigger_page_pool_percent_,
    "When the main page pool runs under this percent (roughly calculated) of free pages,\n"
    " snapshot manager starts snapshotting to drop volatile pages even before the interval.");
  EXTERNALIZE_SAVE_ELEMENT(element, snapshot_interval_milliseconds_,
    "Interval in milliseconds to take snapshots.");
  EXTERNALIZE_SAVE_ELEMENT(element, log_mapper_bucket_kb_,
    "Size in KB of bucket (buffer for each partition) in mapper."
    " The larger, the less freuquently each mapper communicates with reducers."
    " 1024 (1MB) should be a good number.");
  EXTERNALIZE_SAVE_ELEMENT(element, log_mapper_io_buffer_mb_,
    "Size in MB of IO buffer to read log files in mapper."
    " This buffer is also the unit of batch processing in mapper.");
  EXTERNALIZE_SAVE_ELEMENT(element, log_reducer_buffer_mb_,
    "The size in MB of a buffer to store log entries in reducer (partition).");
  EXTERNALIZE_SAVE_ELEMENT(element, log_reducer_dump_io_buffer_mb_,
    "The size in MB of a buffer to write out sorted log entries in reducer to a temporary file.");
  EXTERNALIZE_SAVE_ELEMENT(element, log_reducer_read_io_buffer_kb_,
    "The size in KB of a buffer in reducer to read one temporary file. Note that the total"
    " memory consumption is this number times the number of temporary files. It's a merge-sort.");
  EXTERNALIZE_SAVE_ELEMENT(element, snapshot_writer_page_pool_size_mb_,
    "The size in MB of one snapshot writer, which holds data pages modified in the snapshot"
    " and them sequentially dumps them to a file for each storage.");
  EXTERNALIZE_SAVE_ELEMENT(element, snapshot_writer_intermediate_pool_size_mb_,
    "The size in MB of additional page pool for one snapshot writer just for holding"
    " intermediate pages.");
  CHECK_ERROR(add_child_element(element, "SnapshotDeviceEmulationOptions",
          "[Experiments-only] Settings to emulate slower data device", emulation_));
  return kRetOk;
}

}  // namespace snapshot
}  // namespace foedus
