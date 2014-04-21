/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
namespace foedus {
namespace snapshot {
SnapshotOptions::SnapshotOptions() {
    folder_paths_.push_back(".");
}

ErrorStack SnapshotOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, folder_paths_);
    CHECK_ERROR(get_child_element(element, "EmulationOptions", &emulation_))
    return RET_OK;
}

ErrorStack SnapshotOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for snapshot manager"));

    EXTERNALIZE_SAVE_ELEMENT(element, folder_paths_,
        "Folder paths of snapshot folders.\n"
        " The folders may or may not be on different physical devices."
        " The snapshot folders are used in round-robbin fashion.");
    CHECK_ERROR(add_child_element(element, "XctOptions",
                    "[Experiments-only] Settings to emulate slower data device", emulation_));
    return RET_OK;
}

}  // namespace snapshot
}  // namespace foedus
