/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/partitioner.hpp"

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_partitioner.hpp"

namespace foedus {
namespace storage {
std::ostream& operator<<(std::ostream& o, const Partitioner& v) {
  v.describe(&o);
  return o;
}

Partitioner* Partitioner::create_partitioner(Engine* engine, StorageId id) {
  Storage* storage = engine->get_storage_manager().get_storage(id);
  ASSERT_ND(storage);
  switch (storage->get_type()) {
    case kArrayStorage:
      return new array::ArrayPartitioner(engine, id);
      break;

    // TODO(Hideaki) implement
    case kHashStorage:
    case kMasstreeStorage:
    case kSequentialStorage:
    default:
      break;
  }
  return nullptr;
}

}  // namespace storage
}  // namespace foedus
