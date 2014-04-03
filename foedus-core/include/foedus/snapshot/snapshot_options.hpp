/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
#include <iosfwd>
namespace foedus {
namespace snapshot {
/**
 * @brief Set of options for snapshot manager.
 * @ingroup SNAPSHOT
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct SnapshotOptions {
    /**
     * Constructs option values with default values.
     */
    SnapshotOptions();
};
}  // namespace snapshot
}  // namespace foedus
std::ostream& operator<<(std::ostream& o, const foedus::snapshot::SnapshotOptions& v);
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_OPTIONS_HPP_
