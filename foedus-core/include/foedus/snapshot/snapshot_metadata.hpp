/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_METADATA_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_METADATA_HPP_
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/fwd.hpp"

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
  SnapshotMetadata() { clear(); }
  ~SnapshotMetadata() { clear(); }

  void clear();

  /** Equivalent to Snapshot::id_. */
  SnapshotId  id_;

  /** Equivalent to Snapshot::base_epoch_. */
  Epoch::EpochInteger base_epoch_;

  /** Equivalent to Snapshot::valid_until_epoch_. */
  Epoch::EpochInteger valid_until_epoch_;

  /**
   * @brief metadata of all storages.
   * @details
   * SnapshotMetadata \b owns the pointed objects, so these will be deleted when this vector
   * is cleared. Thus, you must not put a pointer to an existing
   * metadata object owned by existing storage.
   * You should call "clone()" method of them to obtain a copy of it.
   * The reason why SnapshotMetadata these objects is that there aren't any backing storage
   * object of the metadata when we are loading SnapshotMetadata from an xml file.
   * So, SnapshotMetadata must be an independent object.
   * @see foedus::storage::Metadata::clone()
   */
  // std::vector< storage::Metadata* > storage_metadata_;

  EXTERNALIZABLE(SnapshotMetadata);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_METADATA_HPP_
