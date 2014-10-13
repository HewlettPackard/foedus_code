/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {
HashStorage::HashStorage(Engine* engine, const StorageName& name) {
  engine_ = engine;
  control_block_
    = reinterpret_cast<HashStorageControlBlock*>(engine->get_storage_manager()->get_storage(name));
}

ErrorStack  HashStorage::create(const Metadata &metadata) {
  return HashStoragePimpl(this).create(static_cast<const HashMetadata&>(metadata));
}
ErrorStack HashStorage::load(const StorageControlBlock& snapshot_block) {
  return HashStoragePimpl(this).load(snapshot_block);
}
ErrorStack  HashStorage::drop() { return HashStoragePimpl(this).drop(); }

void HashStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<HashStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "<bin_bits>" << static_cast<int>(control_block_->meta_.bin_bits_) << "</bin_bits>"
    << "</HashStorage>";
}
// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace hash
}  // namespace storage
}  // namespace foedus
