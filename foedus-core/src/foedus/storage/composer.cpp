/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/composer.hpp"

#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_composer_impl.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/masstree/masstree_composer_impl.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"
#include "foedus/storage/sequential/sequential_composer_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"

namespace foedus {
namespace storage {
std::ostream& operator<<(std::ostream& o, const Composer& v) {
  v.describe(&o);
  return o;
}

Composer::Composer(
  Engine *engine,
  const Partitioner* partitioner,
  snapshot::SnapshotWriter* snapshot_writer,
  cache::SnapshotFileSet* previous_snapshot_files,
  const snapshot::Snapshot& new_snapshot)
  : engine_(engine),
  partitioner_(partitioner),
  snapshot_writer_(snapshot_writer),
  previous_snapshot_files_(previous_snapshot_files),
  new_snapshot_(new_snapshot),
  new_snapshot_id_(new_snapshot.id_),
  storage_id_(partitioner->get_storage_id()),
  numa_node_(snapshot_writer->get_numa_node()),
  storage_(engine->get_storage_manager().get_storage(storage_id_)),
  previous_root_page_pointer_(storage_->meta_.root_snapshot_page_id_) {
  ASSERT_ND(partitioner);
}

Composer* Composer::create_composer(
  Engine *engine,
  const Partitioner* partitioner,
  snapshot::SnapshotWriter* snapshot_writer,
  cache::SnapshotFileSet* previous_snapshot_files,
  const snapshot::Snapshot& new_snapshot) {
  switch (partitioner->get_storage_type()) {
    case kArrayStorage:
      return new array::ArrayComposer(
        engine,
        dynamic_cast<const array::ArrayPartitioner*>(partitioner),
        snapshot_writer,
        previous_snapshot_files,
        new_snapshot);
      break;

    case kSequentialStorage:
      return new sequential::SequentialComposer(
        engine,
        dynamic_cast<const sequential::SequentialPartitioner*>(partitioner),
        snapshot_writer,
        previous_snapshot_files,
        new_snapshot);
      break;

    case kMasstreeStorage:
      return new masstree::MasstreeComposer(
        engine,
        dynamic_cast<const masstree::MasstreePartitioner*>(partitioner),
        snapshot_writer,
        previous_snapshot_files,
        new_snapshot);
      break;
    // TODO(Hideaki) implement
    case kHashStorage:
    default:
      break;
  }
  return nullptr;
}

}  // namespace storage
}  // namespace foedus
