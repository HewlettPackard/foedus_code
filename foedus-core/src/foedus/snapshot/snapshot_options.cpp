/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/snapshot/snapshot_options.hpp>
#include <ostream>
namespace foedus {
namespace snapshot {
SnapshotOptions::SnapshotOptions() {
    folder_paths_.push_back(".");
}

}  // namespace snapshot
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::snapshot::SnapshotOptions& v) {
    o << "Snapshot options:" << std::endl;
    for (size_t i = 0; i < v.folder_paths_.size(); ++i) {
        o << "  folder_paths[" << i << "]=" << v.folder_paths_[i] << std::endl;
    }
    o << v.emulation_;
    return o;
}
