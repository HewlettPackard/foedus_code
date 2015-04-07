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
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_METADATA_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_METADATA_HPP_
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace snapshot {

/**
 * @brief Represents the data in one snapshot metadata file.
 * @ingroup SNAPSHOT
 * @details
 * One snapshot metadata file is written for each snapshotting as an xml file.
 * It contains metadata of all storages and a few other global things.
 *
 * We write it out as part of snapshotting.
 * We read it at restart.
 */
struct SnapshotMetadata CXX11_FINAL : public virtual externalize::Externalizable {
  void clear();
  storage::Metadata* get_metadata(storage::StorageId id) {
    return &storage_control_blocks_[id].meta_;
  }

  ErrorStack load(tinyxml2::XMLElement* element) CXX11_OVERRIDE;
  ErrorStack save(tinyxml2::XMLElement* element) const CXX11_OVERRIDE;
  const char* get_tag_name() const CXX11_OVERRIDE { return "SnapshotMetadata"; }
  void assign(const foedus::externalize::Externalizable *other) CXX11_OVERRIDE;

  /** Equivalent to Snapshot::id_. */
  SnapshotId  id_;

  /** Equivalent to Snapshot::base_epoch_. */
  Epoch::EpochInteger base_epoch_;

  /** Equivalent to Snapshot::valid_until_epoch_. */
  Epoch::EpochInteger valid_until_epoch_;

  /** The largest StorageId we so far observed. */
  storage::StorageId  largest_storage_id_;

  /**
   * @brief control block of all storages.
   * @details
   * This is a copy of the shared memory, so no need to delete.
   */
  storage::StorageControlBlock* storage_control_blocks_;

  /** Memory backing storage_control_blocks_ */
  memory::AlignedMemory storage_control_blocks_memory_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_METADATA_HPP_
