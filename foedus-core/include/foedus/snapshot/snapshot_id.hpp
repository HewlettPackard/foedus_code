/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_ID_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_ID_HPP_
#include <foedus/assert_nd.hpp>
#include <stdint.h>
/**
 * @file foedus/snapshot/snapshot_id.hpp
 * @brief Typedefs of ID types used in snapshot package.
 * @ingroup SNAPSHOT
 */
namespace foedus {
namespace snapshot {
/**
 * @brief Unique ID of Snapshot.
 * @ingroup SNAPSHOT
 * @details
 * Snapshot ID is a 16-bit integer.
 * As we periodically merge all snapshots, we won't have 2^16 snapshots at one time.
 * This ID wraps around, but it causes no issue as we never compare greater-than/less-than between
 * snapshot ID. All snapshots contain base and valid_until epochs, so we just compare them.
 *
 * ID-0 is a special value that means NULL. Use the following method to increment a snapshot ID to
 * preserve this invariant.
 */
typedef uint16_t SnapshotId;

const SnapshotId NULL_SNAPSHOT_ID = 0;

/**
 * @brief Increment SnapshotId.
 * @ingroup SNAPSHOT
 * @invariant id != NULL_SNAPSHOT_ID
 */
inline SnapshotId increment(SnapshotId id) {
    ASSERT_ND(id != NULL_SNAPSHOT_ID);
    ++id;
    if (id == NULL_SNAPSHOT_ID) {
        return 1;  // wrap around, and skip 0.
    } else {
        return id;
    }
}

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_ID_HPP_
