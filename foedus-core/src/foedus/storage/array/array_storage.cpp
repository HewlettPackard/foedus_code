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

ArrayStorage::ArrayStorage() : Storage<ArrayStorageControlBlock>() {}
ArrayStorage::ArrayStorage(Engine* engine, ArrayStorageControlBlock* control_block)
  : Storage<ArrayStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kArrayStorage || !exists());
}
ArrayStorage::ArrayStorage(Engine* engine, StorageControlBlock* control_block)
  : Storage<ArrayStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kArrayStorage || !exists());
}
ArrayStorage::ArrayStorage(Engine* engine, StorageId id)
  : Storage<ArrayStorageControlBlock>(engine, id) {}
ArrayStorage::ArrayStorage(Engine* engine, const StorageName& name)
  : Storage<ArrayStorageControlBlock>(engine, name) {}
ArrayStorage::ArrayStorage(const ArrayStorage& other)
  : Storage<ArrayStorageControlBlock>(other.engine_, other.control_block_) {
}
ArrayStorage& ArrayStorage::operator=(const ArrayStorage& other) {
  engine_ = other.engine_;
  control_block_ = other.control_block_;
  return *this;
}
ErrorStack ArrayStorage::create(const Metadata& metadata) {
  return ArrayStoragePimpl(this).create(metadata);
}

ErrorStack ArrayStorage::load(const StorageControlBlock& snapshot_block) {
  return ArrayStoragePimpl(this).load(snapshot_block);
}

std::ostream& operator<<(std::ostream& o, const ArrayStorage& v) {
  o << "<ArrayStorage>"
    << "<id>" << v.get_id() << "</id>"
    << "<name>" << v.get_name() << "</name>"
    << "<payload_size>" << v.get_payload_size() << "</payload_size>"
    << "<array_size>" << v.get_array_size() << "</array_size>"
    << "</ArrayStorage>";
  return o;
}


ErrorCode   ArrayStorage::prefetch_pages(
  thread::Thread* context,
  bool install_volatile,
  bool cache_snapshot,
  ArrayOffset from,
  ArrayOffset to) {
  if (to == 0) {
    to = get_array_size();
  }
  return ArrayStoragePimpl(this).prefetch_pages(
    context,
    install_volatile,
    cache_snapshot,
    from,
    to);
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace array
}  // namespace storage
}  // namespace foedus
