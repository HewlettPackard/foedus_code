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
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_composer_impl.hpp"
#include "foedus/storage/sequential/sequential_composer_impl.hpp"

namespace foedus {
namespace storage {
std::ostream& operator<<(std::ostream& o, const Composer& v) {
  v.describe(&o);
  return o;
}

Composer::Composer(
  Engine *engine,
  StorageId storage_id,
  snapshot::SnapshotWriter* snapshot_writer,
  cache::SnapshotFileSet* previous_snapshot_files,
  snapshot::SnapshotId new_snapshot_id)
  : engine_(engine),
  snapshot_writer_(snapshot_writer),
  previous_snapshot_files_(previous_snapshot_files),
  new_snapshot_id_(new_snapshot_id),
  storage_id_(storage_id),
  numa_node_(snapshot_writer->get_numa_node()),
  storage_(engine->get_storage_manager()->get_storage(storage_id_)),
  previous_root_page_pointer_(storage_->meta_.root_snapshot_page_id_) {
}

Composer* Composer::create_composer(
  Engine *engine,
  StorageId storage_id,
  snapshot::SnapshotWriter* snapshot_writer,
  cache::SnapshotFileSet* previous_snapshot_files,
  snapshot::SnapshotId new_snapshot_id) {
  switch (engine->get_storage_manager()->get_storage(storage_id)->meta_.type_) {
    case kArrayStorage:
      return new array::ArrayComposer(
        engine,
        storage_id,
        snapshot_writer,
        previous_snapshot_files,
        new_snapshot_id);
      break;

    case kSequentialStorage:
      return new sequential::SequentialComposer(
        engine,
        storage_id,
        snapshot_writer,
        previous_snapshot_files,
        new_snapshot_id);
      break;

    // TODO(Hideaki) implement
    case kMasstreeStorage:
    case kHashStorage:
    default:
      break;
  }
  return nullptr;
}

}  // namespace storage
}  // namespace foedus
