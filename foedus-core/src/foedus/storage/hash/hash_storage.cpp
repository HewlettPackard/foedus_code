/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {

HashStorage::HashStorage() : Storage<HashStorageControlBlock>() {}
HashStorage::HashStorage(Engine* engine, HashStorageControlBlock* control_block)
  : Storage<HashStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kHashStorage || !exists());
}
HashStorage::HashStorage(Engine* engine, StorageControlBlock* control_block)
  : Storage<HashStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kHashStorage || !exists());
}
HashStorage::HashStorage(Engine* engine, StorageId id)
  : Storage<HashStorageControlBlock>(engine, id) {}
HashStorage::HashStorage(Engine* engine, const StorageName& name)
  : Storage<HashStorageControlBlock>(engine, name) {}
HashStorage::HashStorage(const HashStorage& other)
  : Storage<HashStorageControlBlock>(other.engine_, other.control_block_) {
}
HashStorage& HashStorage::operator=(const HashStorage& other) {
  engine_ = other.engine_;
  control_block_ = other.control_block_;
  return *this;
}


ErrorStack  HashStorage::create(const Metadata &metadata) {
  return HashStoragePimpl(this).create(static_cast<const HashMetadata&>(metadata));
}
ErrorStack HashStorage::load(const StorageControlBlock& snapshot_block) {
  return HashStoragePimpl(this).load(snapshot_block);
}
ErrorStack  HashStorage::drop() { return HashStoragePimpl(this).drop(); }

std::ostream& operator<<(std::ostream& o, const HashStorage& v) {
  o << "<HashStorage>"
    << "<id>" << v.get_id() << "</id>"
    << "<name>" << v.get_name() << "</name>"
    << "<bin_bits>" << static_cast<int>(v.control_block_->meta_.bin_bits_) << "</bin_bits>"
    << "</HashStorage>";
  return o;
}
// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace hash
}  // namespace storage
}  // namespace foedus
