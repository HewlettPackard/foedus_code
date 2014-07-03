/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/composer.hpp"

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/array/array_composer_impl.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/sequential/sequential_composer_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"

namespace foedus {
namespace storage {
std::ostream& operator<<(std::ostream& o, const Composer& v) {
  v.describe(&o);
  return o;
}

Composer* Composer::create_composer(
  Engine *engine,
  const Partitioner* partitioner,
  snapshot::SnapshotWriter* snapshot_writer,
  const snapshot::Snapshot& new_snapshot) {
  switch (partitioner->get_storage_type()) {
    case kArrayStorage:
      return new array::ArrayComposer(
        engine,
        dynamic_cast<const array::ArrayPartitioner*>(partitioner),
        snapshot_writer,
        new_snapshot);
      break;

    case kSequentialStorage:
      return new sequential::SequentialComposer(
        engine,
        dynamic_cast<const sequential::SequentialPartitioner*>(partitioner),
        snapshot_writer,
        new_snapshot);
      break;
    // TODO(Hideaki) implement
    case kHashStorage:
    case kMasstreeStorage:
    default:
      break;
  }
  return nullptr;
}

}  // namespace storage
}  // namespace foedus
