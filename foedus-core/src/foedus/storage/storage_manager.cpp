/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include "foedus/storage/storage_manager.hpp"

#include <string>

#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"

namespace foedus {
namespace storage {
StorageManager::StorageManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new StorageManagerPimpl(engine);
}
StorageManager::~StorageManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

Engine*     StorageManager::get_engine() const { return pimpl_->engine_; }
ErrorStack  StorageManager::initialize() { return pimpl_->initialize(); }
bool        StorageManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  StorageManager::uninitialize() { return pimpl_->uninitialize(); }

StorageControlBlock* StorageManager::get_storage(StorageId id) {
  return pimpl_->get_storage(id);
}
StorageControlBlock* StorageManager::get_storage(const StorageName& name) {
  return pimpl_->get_storage(name);
}

StorageId StorageManager::issue_next_storage_id() { return pimpl_->issue_next_storage_id(); }
StorageId StorageManager::get_largest_storage_id() {
  return pimpl_->control_block_->largest_storage_id_;
}

const StorageName kEmptyString;
const StorageName& StorageManager::get_name(StorageId id) {
  StorageControlBlock* block = get_storage(id);
  if (block->exists()) {
    return block->meta_.name_;
  } else {
    return kEmptyString;
  }
}

ErrorStack StorageManager::drop_storage(StorageId id, Epoch *commit_epoch) {
  return pimpl_->drop_storage(id, commit_epoch);
}
void StorageManager::drop_storage_apply(StorageId id) {
  pimpl_->drop_storage_apply(id);
}

ErrorStack StorageManager::create_storage(Metadata *metadata, Epoch *commit_epoch) {
  return pimpl_->create_storage(metadata, commit_epoch);
}
void StorageManager::create_storage_apply(const Metadata& metadata) {
  pimpl_->create_storage_apply(metadata);
}

ErrorStack StorageManager::create_array(
  array::ArrayMetadata* metadata,
  array::ArrayStorage* storage,
  Epoch* commit_epoch) {
  CHECK_ERROR(create_storage(metadata, commit_epoch));
  *storage = get_array(metadata->id_);
  return kRetOk;
}

ErrorStack StorageManager::create_hash(
  hash::HashMetadata* metadata,
  hash::HashStorage* storage,
  Epoch* commit_epoch) {
  CHECK_ERROR(create_storage(metadata, commit_epoch));
  *storage = get_hash(metadata->id_);
  return kRetOk;
}

ErrorStack StorageManager::create_sequential(
  sequential::SequentialMetadata* metadata,
  sequential::SequentialStorage* storage,
  Epoch* commit_epoch) {
  CHECK_ERROR(create_storage(metadata, commit_epoch));
  *storage = get_sequential(metadata->id_);
  return kRetOk;
}

ErrorStack StorageManager::create_masstree(
  masstree::MasstreeMetadata* metadata,
  masstree::MasstreeStorage* storage,
  Epoch* commit_epoch) {
  CHECK_ERROR(create_storage(metadata, commit_epoch));
  *storage = get_masstree(metadata->id_);
  return kRetOk;
}

ErrorStack StorageManager::clone_all_storage_metadata(snapshot::SnapshotMetadata *metadata) {
  return pimpl_->clone_all_storage_metadata(metadata);
}

}  // namespace storage
}  // namespace foedus
