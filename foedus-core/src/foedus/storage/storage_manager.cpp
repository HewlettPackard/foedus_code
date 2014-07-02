/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage_manager.hpp"

#include <string>

#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
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

ErrorStack  StorageManager::initialize() { return pimpl_->initialize(); }
bool        StorageManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  StorageManager::uninitialize() { return pimpl_->uninitialize(); }

Storage* StorageManager::get_storage(StorageId id) { return pimpl_->get_storage(id); }
Storage* StorageManager::get_storage(const std::string& name) { return pimpl_->get_storage(name); }

StorageId StorageManager::issue_next_storage_id() { return pimpl_->issue_next_storage_id(); }
ErrorStack StorageManager::drop_storage(thread::Thread* context, StorageId id,
                    Epoch *commit_epoch) {
  return pimpl_->drop_storage(context, id, commit_epoch);
}
ErrorStack StorageManager::drop_storage(StorageId id, Epoch *commit_epoch) {
  return pimpl_->drop_storage(id, commit_epoch);
}

ErrorStack StorageManager::create_storage(thread::Thread* context, Metadata *metadata,
    Storage **storage, Epoch *commit_epoch) {
  return pimpl_->create_storage(context, metadata, storage, commit_epoch);
}
ErrorStack StorageManager::create_storage(Metadata *metadata, Storage **storage,
    Epoch *commit_epoch) {
  return pimpl_->create_storage(metadata, storage, commit_epoch);
}

ErrorStack StorageManager::create_array(
  thread::Thread* context,
  array::ArrayMetadata* metadata,
  array::ArrayStorage** storage,
  Epoch* commit_epoch) {
  Storage* tmp = nullptr;
  ErrorStack result = create_storage(context, metadata, &tmp, commit_epoch);
  *storage = dynamic_cast<array::ArrayStorage*>(tmp);
  return result;
}

ErrorStack StorageManager::create_array(
  array::ArrayMetadata* metadata,
  array::ArrayStorage** storage,
  Epoch* commit_epoch) {
  Storage* tmp = nullptr;
  ErrorStack result = create_storage(metadata, &tmp, commit_epoch);
  *storage = dynamic_cast<array::ArrayStorage*>(tmp);
  return result;
}


ErrorStack StorageManager::create_sequential(
  thread::Thread* context,
  sequential::SequentialMetadata* metadata,
  sequential::SequentialStorage** storage,
  Epoch* commit_epoch) {
  Storage* tmp = nullptr;
  ErrorStack result = create_storage(context, metadata, &tmp, commit_epoch);
  *storage = dynamic_cast<sequential::SequentialStorage*>(tmp);
  return result;
}

ErrorStack StorageManager::create_sequential(
  sequential::SequentialMetadata* metadata,
  sequential::SequentialStorage** storage,
  Epoch* commit_epoch) {
  Storage* tmp = nullptr;
  ErrorStack result = create_storage(metadata, &tmp, commit_epoch);
  *storage = dynamic_cast<sequential::SequentialStorage*>(tmp);
  return result;
}

ErrorStack StorageManager::clone_all_storage_metadata(snapshot::SnapshotMetadata *metadata) {
  return pimpl_->clone_all_storage_metadata(metadata);
}

}  // namespace storage
}  // namespace foedus
