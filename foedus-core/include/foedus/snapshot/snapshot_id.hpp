/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_ID_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_ID_HPP_
#include <stdint.h>

#include "foedus/assert_nd.hpp"

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

/**
 * @brief Unique ID of Partition of snapshot files.
 * @ingroup SNAPSHOT
 * @details
 * Snapshot files are stored in partitions.
 * Each NUMA node exclusively owns one or more partition.
 * All storages partition their data into one of the partition without overlaps.
 * PartitionId is merely a (partitions_per_node * NUMA_node_id) + partition_ordinal_in_node.
 * Assumeing that there are at most 256 partitions per NUMA node, it always fits 16 bits.
 */
typedef uint16_t PartitionId;

const SnapshotId kNullSnapshotId = 0;

/**
 * @brief Increment SnapshotId.
 * @ingroup SNAPSHOT
 * @invariant id != kNullSnapshotId
 */
inline SnapshotId increment(SnapshotId id) {
  ASSERT_ND(id != kNullSnapshotId);
  ++id;
  if (id == kNullSnapshotId) {
    return 1;  // wrap around, and skip 0.
  } else {
    return id;
  }
}

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_ID_HPP_
