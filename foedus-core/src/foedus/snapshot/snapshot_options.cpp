/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
#include <string>
namespace foedus {
namespace snapshot {
SnapshotOptions::SnapshotOptions() {
    folder_path_pattern_ = "snapshots/node_$NODE$/partition_$PARTITION$";
    partitions_per_node_ = 1;
    snapshot_trigger_page_pool_percent_ = kDefaultSnapshotTriggerPagePoolPercent;
    snapshot_interval_milliseconds_ = kDefaultSnapshotIntervalMilliseconds;
}

std::string SnapshotOptions::convert_folder_path_pattern(int node, int partition) const {
    std::string tmp = assorted::replace_all(folder_path_pattern_, "$NODE$", node);
    return assorted::replace_all(tmp, "$PARTITION$", partition);
}

ErrorStack SnapshotOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, folder_path_pattern_);
    EXTERNALIZE_LOAD_ELEMENT(element, partitions_per_node_);
    EXTERNALIZE_LOAD_ELEMENT(element, snapshot_trigger_page_pool_percent_);
    EXTERNALIZE_LOAD_ELEMENT(element, snapshot_interval_milliseconds_);
    CHECK_ERROR(get_child_element(element, "SnapshotDeviceEmulationOptions", &emulation_))
    return kRetOk;
}

ErrorStack SnapshotOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for snapshot manager"));

    EXTERNALIZE_SAVE_ELEMENT(element, folder_path_pattern_,
        "String pattern of path of snapshot folders in each NUMA node.\n"
        "This specifies the path of the folders to contain snapshot files in each NUMA node.\n"
        " Two special placeholders can be used; $NODE$ and $PARTITION$."
        " $NODE$ is replaced with the NUMA node number.\n"
        " $PARTITION$ is replaced with the partition in the node (0 to partitions_per_node_ - 1)."
        " For example,\n"
        " /data/node_$NODE$/folder_$INDEX$ becomes /data/node_1/folder_0 on node-1 and index-0.\n"
        " /data/folder_$INDEX$ becomes /data/folder_1 on any node and index-1.\n"
        " Both are optional. You can specify a fixed path without the patterns, which means you\n"
        " will use the same folder for multiple partitions or nodes. Even in that case, snapshot\n"
        " file names include uniquefiers, so it wouldn't cause any data corruption. It just makes\n"
        " things harder for poor sysadmins.\n"
        " The snapshot folders are also the granularity of partitioning."
        " Each snapshot phase starts with partitioning of logs using random samples, then"
        " scatter-gather log entries to assigned partitions like Map-Reduce.");
    EXTERNALIZE_SAVE_ELEMENT(element, partitions_per_node_,
        "Number of snapshot folders (ie partitions) per NUMA node.\n"
        "This value must be at least 1 (which is also default)."
        " A larger value might be able to employ more CPU power during snapshot construction,"
        " but makes the scatter-gather more fine grained, potentially making it slower.");
    EXTERNALIZE_SAVE_ELEMENT(element, snapshot_trigger_page_pool_percent_,
        "When the main page pool runs under this percent (roughly calculated) of free pages,\n"
        " snapshot manager starts snapshotting to drop volatile pages even before the interval.");
    EXTERNALIZE_SAVE_ELEMENT(element, snapshot_interval_milliseconds_,
        "Interval in milliseconds to take snapshots.");
    CHECK_ERROR(add_child_element(element, "SnapshotDeviceEmulationOptions",
                    "[Experiments-only] Settings to emulate slower data device", emulation_));
    return kRetOk;
}

}  // namespace snapshot
}  // namespace foedus
