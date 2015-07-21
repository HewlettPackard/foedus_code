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

/**
 * @brief Represents a position in some buffer.
 * @ingroup SNAPSHOT
 * @details
 * As log is always 8-byte aligned, we divide the original byte position by 8.
 * Thus, this can represent up to 8 * 2^32=32GB, which is the maximum value of
 * log_mapper_io_buffer_mb_.
 * @see to_buffer_position
 * @see from_buffer_position
 */
typedef uint32_t BufferPosition;

inline BufferPosition to_buffer_position(uint64_t byte_position) {
  ASSERT_ND(byte_position % 8 == 0);
  return byte_position >> 3;
}
inline uint64_t from_buffer_position(BufferPosition buffer_position) {
  return static_cast<uint64_t>(buffer_position) << 3;
}
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_ID_HPP_
