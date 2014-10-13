/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/composer.hpp"

#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
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
  o << "<Composer>"
    << "<storage_id_>" << v.get_storage_id() << "</storage_id_>"
    << "<storage_type>" << to_storage_type_name(v.get_storage_type()) << "</storage_type>"
    << "</Composer>";
  return o;
}

Composer::Composer(Engine *engine, StorageId storage_id)
  : engine_(engine),
    storage_id_(storage_id),
    storage_type_(engine_->get_storage_manager()->get_storage(storage_id_)->meta_.type_) {}


ErrorStack Composer::compose(const ComposeArguments& args) {
  switch (storage_type_) {
    case kArrayStorage: return array::ArrayComposer(this).compose(args);
    case kSequentialStorage: return sequential::SequentialComposer(this).compose(args);
    // TODO(Hideaki) implement
    case kMasstreeStorage:
    case kHashStorage:
    default:
      return kRetOk;
  }
}

ErrorStack Composer::construct_root(const ConstructRootArguments& args) {
  switch (storage_type_) {
    case kArrayStorage: return array::ArrayComposer(this).construct_root(args);
    case kSequentialStorage: return sequential::SequentialComposer(this).construct_root(args);
    // TODO(Hideaki) implement
    case kMasstreeStorage:
    case kHashStorage:
    default:
      return kRetOk;
  }
}

ErrorStack Composer::replace_pointers(const ReplacePointersArguments& args) {
  switch (storage_type_) {
    case kArrayStorage:  return array::ArrayComposer(this).replace_pointers(args);
    case kSequentialStorage: return sequential::SequentialComposer(this).replace_pointers(args);
    // TODO(Hideaki) implement
    case kMasstreeStorage:
    case kHashStorage:
    default:
      return kRetOk;
  }
}

void Composer::ReplacePointersArguments::drop_volatile_page(VolatilePagePointer pointer) const {
  uint16_t node = pointer.components.numa_node;
  ASSERT_ND(node < snapshot_files_->get_engine()->get_soc_count());
  ASSERT_ND(pointer.components.offset > 0);
  memory::PagePoolOffsetChunk* chunk = dropped_chunks_ + node;
  if (chunk->full()) {
    Engine* engine = snapshot_files_->get_engine();
    memory::PagePool* pool
      = engine->get_memory_manager()->get_node_memory(node)->get_volatile_pool();
    pool->release(chunk->size(), chunk);
    ASSERT_ND(chunk->empty());
  }
  ASSERT_ND(!chunk->full());
  chunk->push_back(pointer.components.offset);
  ++(*dropped_count_);
}

ErrorCode Composer::ReplacePointersArguments::read_snapshot_page(
  SnapshotPagePointer pointer,
  void* out) const {
  return snapshot_files_->read_page(pointer, out);
}

}  // namespace storage
}  // namespace foedus
