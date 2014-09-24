/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/partitioner.hpp"

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"

namespace foedus {
namespace storage {
std::ostream& operator<<(std::ostream& o, const Partitioner& v) {
  v.describe(&o);
  return o;
}

Partitioner* Partitioner::create_partitioner(Engine* engine, StorageId id) {
  StorageControlBlock* block = engine->get_storage_manager().get_storage(id);
  ASSERT_ND(block->exists());
  switch (block->meta_.type_) {
    case kArrayStorage:
      return new array::ArrayPartitioner(engine, id);
      break;

    case kSequentialStorage:
      return new sequential::SequentialPartitioner(id);
      break;

    case kMasstreeStorage:
      return new masstree::MasstreePartitioner(id);
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
