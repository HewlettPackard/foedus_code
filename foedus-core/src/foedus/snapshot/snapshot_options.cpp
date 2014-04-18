/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
#include <ostream>
namespace foedus {
namespace snapshot {
SnapshotOptions::SnapshotOptions() {
    folder_paths_.push_back(".");
}

std::ostream& operator<<(std::ostream& o, const SnapshotOptions& v) {
    o << "  <SnapshotOptions>" << std::endl;
    EXTERNALIZE_WRITE(folder_paths_);
    o << v.emulation_;
    o << "  </SnapshotOptions>" << std::endl;
    return o;
}

}  // namespace snapshot
}  // namespace foedus
