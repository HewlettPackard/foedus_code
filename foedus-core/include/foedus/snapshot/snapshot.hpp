/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_HPP_
#include <foedus/epoch.hpp>
#include <foedus/snapshot/snapshot_id.hpp>
#include <iosfwd>
namespace foedus {
namespace snapshot {

/**
 * @brief Represents one snapshot that converts all logs from base epoch to valid_until epoch
 * into snapshot file(s).
 * @ingroup SNAPSHOT
 * @details
 * This is POD.
 */
struct Snapshot {
  /**
   * Unique ID of this snapshot.
   * @attention Greater-than/less-than has no meaning due to wrap-around. Use epochs for that
   * purpose. ID is used only for equality.
   */
  SnapshotId  id_;

  /**
   * This snapshot was taken based on another snapshot that is valid_until this epoch.
   * If this is the first snapshot, this is an invalid epoch.
   */
  Epoch base_epoch_;

  /**
   * This snapshot contains all the logs until this epoch.
   * @invariant valid_until_epoch_.is_valid()
   */
  Epoch valid_until_epoch_;

  // here goes statistics, but it's not yet done.

  friend std::ostream& operator<<(std::ostream& o, const Snapshot& v);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_HPP_
