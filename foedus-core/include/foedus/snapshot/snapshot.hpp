/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_HPP_

#include <iosfwd>

#include "foedus/epoch.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace snapshot {

/**
 * @brief Represents one snapshot that converts all logs from base epoch to valid_until epoch
 * into snapshot file(s).
 * @ingroup SNAPSHOT
 * @details
 * This is POD and no heap-allocated data, so it can be directly placed in shared memory.
 */
struct Snapshot {
  /**
   * Unique ID of this snapshot.
   * @attention Greater-than/less-than has no meaning due to wrap-around. Use epochs for that
   * purpose. ID is used only for equality.
   */
  SnapshotId  id_;

  /**
   * This snapshot was taken on top of previous snapshot that is valid_until this epoch.
   * If this is the first snapshot, this is an invalid epoch.
   */
  Epoch base_epoch_;

  /**
   * This snapshot contains all the logs until this epoch.
   * @invariant valid_until_epoch_.is_valid()
   */
  Epoch valid_until_epoch_;

  /** Largest storage ID as of starting to take the snapshot. */
  storage::StorageId max_storage_id_;

  friend std::ostream& operator<<(std::ostream& o, const Snapshot& v);
  void clear() {
    id_ = 0;
    base_epoch_ = INVALID_EPOCH;
    valid_until_epoch_ = INVALID_EPOCH;
    max_storage_id_ = 0;
  }
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_HPP_
