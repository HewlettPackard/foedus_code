/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
namespace foedus {
namespace snapshot {
SnapshotOptions::SnapshotOptions() {
    folder_path_pattern_ = "snapshots/node_$NODE$/partition_$PARTITION$";
    partitions_per_node_ = 1;
}

ErrorStack SnapshotOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, folder_path_pattern_);
    CHECK_ERROR(get_child_element(element, "SnapshotDeviceEmulationOptions", &emulation_))
    return RET_OK;
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
    CHECK_ERROR(add_child_element(element, "SnapshotDeviceEmulationOptions",
                    "[Experiments-only] Settings to emulate slower data device", emulation_));
    return RET_OK;
}

}  // namespace snapshot
}  // namespace foedus
