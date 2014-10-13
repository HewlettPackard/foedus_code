/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_storage.hpp"

#include <ostream>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace array {
ArrayStorage::ArrayStorage(Engine* engine, StorageId id) {
  engine_ = engine;
  control_block_
    = reinterpret_cast<ArrayStorageControlBlock*>(engine->get_storage_manager()->get_storage(id));
}

ArrayStorage::ArrayStorage(Engine* engine, const StorageName& name) {
  engine_ = engine;
  control_block_
    = reinterpret_cast<ArrayStorageControlBlock*>(engine->get_storage_manager()->get_storage(name));
}

ErrorStack ArrayStorage::load(const StorageControlBlock& snapshot_block) {
  return ArrayStoragePimpl(this).load(snapshot_block);
}

void ArrayStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<ArrayStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "<payload_size>" << get_payload_size() << "</payload_size>"
    << "<array_size>" << get_array_size() << "</array_size>"
    << "</ArrayStorage>";
}


ErrorStack ArrayStorage::replace_pointers(const Composer::ReplacePointersArguments& args) {
  return ArrayStoragePimpl(this).replace_pointers(args);
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace array
}  // namespace storage
}  // namespace foedus
