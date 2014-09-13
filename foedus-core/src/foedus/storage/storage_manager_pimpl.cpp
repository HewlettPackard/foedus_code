/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage_manager_pimpl.hpp"

#include <glog/logging.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/snapshot/snapshot_metadata.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/storage_options.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
uint32_t   StorageManagerPimpl::get_max_storages() const {
  return engine_->get_options().storage_.max_storages_;
}

ErrorStack StorageManagerPimpl::initialize_once() {
  LOG(INFO) << "Initializing StorageManager..";
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_log_manager().is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }

  largest_storage_id_ = 0;
  storages_ = nullptr;
  init_storage_factories();
  LOG(INFO) << "We have " << storage_factories_.size() << " types of storages";

  uint32_t storages_capacity = engine_->get_options().storage_.max_storages_;
  storages_ = new Storage*[storages_capacity];
  if (!storages_) {
    return ERROR_STACK(kErrorCodeOutofmemory);
  }
  std::memset(storages_, 0, sizeof(Storage*) * storages_capacity);

  max_storages_ = engine_->get_options().storage_.max_storages_;
  instance_memory_.alloc(
    static_cast<uint64_t>(max_storages_) * kPageSize,
    1 << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    0,
    true);

  return kRetOk;
}

ErrorStack StorageManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing StorageManager..";
  ErrorStackBatch batch;
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_log_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  uint32_t storages_capacity = engine_->get_options().storage_.max_storages_;
  for (size_t i = 0; i < storages_capacity; ++i) {
    if (storages_[i] && storages_[i]->is_initialized()) {
      LOG(INFO) << "Invoking uninitialization for storage-" << i
        << "(" << storages_[i]->get_name() << ")";
      batch.emprace_back(storages_[i]->uninitialize());
    }
  }
  {
    std::lock_guard<std::mutex> guard(mod_lock_);
    for (auto it = storage_names_.begin(); it != storage_names_.end(); ++it) {
      delete it->second;
    }
    storage_names_.clear();
    delete[] storages_;
    storages_ = nullptr;
  }
  clear_storage_factories();
  instance_memory_.release_block();
  return SUMMARIZE_ERROR_BATCH(batch);
}

void StorageManagerPimpl::init_storage_factories() {
  clear_storage_factories();
  // list all storage factories here
  storage_factories_.push_back(new array::ArrayStorageFactory());
  storage_factories_.push_back(new masstree::MasstreeStorageFactory());
  storage_factories_.push_back(new sequential::SequentialStorageFactory());
  storage_factories_.push_back(new hash::HashStorageFactory());
}

void StorageManagerPimpl::clear_storage_factories() {
  for (StorageFactory* factory : storage_factories_) {
    delete factory;
  }
  storage_factories_.clear();
}

StorageId StorageManagerPimpl::issue_next_storage_id() {
  std::lock_guard<std::mutex> guard(mod_lock_);  // implies fence too
  ++largest_storage_id_;
  LOG(INFO) << "Incremented largest_storage_id_: " << largest_storage_id_;
  return largest_storage_id_;
}

Storage* StorageManagerPimpl::get_storage(StorageId id) {
  assorted::memory_fence_acquire();  // to sync with expand
  return storages_[id];
}
Storage* StorageManagerPimpl::get_storage(const StorageName& name) {
  std::lock_guard<std::mutex> guard(mod_lock_);
  auto it = storage_names_.find(name);
  if (it == storage_names_.end()) {
    LOG(WARNING) << "Requested storage name '" << name << "' was not found";
    return nullptr;
  } else {
    return it->second;
  }
}


ErrorStack StorageManagerPimpl::register_storage(Storage* storage) {
  ASSERT_ND(storage);
  ASSERT_ND(storage->is_initialized());
  StorageId id = storage->get_id();
  LOG(INFO) << "Adding storage of ID-" << id << "(" << storage->get_name() << ")";
  if (get_max_storages() <= id) {
    return ERROR_STACK(kErrorCodeStrTooManyStorages);
  }

  ASSERT_ND(get_max_storages() > id);
  std::lock_guard<std::mutex> guard(mod_lock_);
  if (storages_[id]) {
    LOG(ERROR) << "Duplicate register_storage() call? ID=" << id;
    return ERROR_STACK(kErrorCodeStrDuplicateStrid);
  }
  if (storage_names_.find(storage->get_name()) != storage_names_.end()) {
    LOG(ERROR) << "Duplicate register_storage() call? Name=" << storage->get_name();
    return ERROR_STACK(kErrorCodeStrDuplicateStrname);
  }
  storages_[id] = storage;
  storage_names_.insert(std::pair< StorageName, Storage* >(storage->get_name(), storage));
  if (id > largest_storage_id_) {
    largest_storage_id_ = id;
  }
  return kRetOk;
}

ErrorStack StorageManagerPimpl::drop_storage(thread::Thread* context, StorageId id,
                       Epoch *commit_epoch) {
  // DROP STORAGE must be the only log in this transaction
  if (context->get_thread_log_buffer().get_offset_committed() !=
    context->get_thread_log_buffer().get_offset_tail()) {
    return ERROR_STACK(kErrorCodeStrMustSeparateXct);
  }

  // to avoid mixing with normal operations on the storage in this epoch, advance epoch now.
  engine_->get_xct_manager().advance_current_global_epoch();

  CHECK_ERROR(engine_->get_xct_manager().begin_schema_xct(context));

  // write out log
  DropLogType* log_entry = reinterpret_cast<DropLogType*>(
    context->get_thread_log_buffer().reserve_new_log(sizeof(DropLogType)));
  log_entry->populate(id);

  // commit invokes apply
  CHECK_ERROR(engine_->get_xct_manager().precommit_xct(context, commit_epoch));
  return kRetOk;
}
void StorageManagerPimpl::drop_storage_apply(thread::Thread* /*context*/, Storage* storage) {
  ASSERT_ND(storage);
  StorageId id = storage->get_id();
  StorageName name = storage->get_name();
  LOG(INFO) << "Dropping storage " << id << "(" << name << ")";
  COERCE_ERROR(storage->uninitialize());
  LOG(INFO) << "Uninitialized storage " << id << "(" << name << ")";

  std::lock_guard<std::mutex> guard(mod_lock_);
  storage_names_.erase(name);
  storages_[id] = nullptr;
  delete storage;
  LOG(INFO) << "Droped storage " << id << "(" << name << ")";
}

/** A task to drop a task. Used from impersonate version of drop_storage(). */
class DropStorageTask final : public foedus::thread::ImpersonateTask {
 public:
  DropStorageTask(StorageManagerPimpl* pimpl, StorageId id, Epoch *commit_epoch)
    : pimpl_(pimpl), id_(id), commit_epoch_(commit_epoch) {}
  ErrorStack run(thread::Thread* context) override {
    CHECK_ERROR(pimpl_->drop_storage(context, id_, commit_epoch_));
    return kRetOk;
  }

 private:
  StorageManagerPimpl* pimpl_;
  StorageId id_;
  Epoch *commit_epoch_;
};

ErrorStack StorageManagerPimpl::drop_storage(StorageId id, Epoch *commit_epoch) {
  DropStorageTask task(this, id, commit_epoch);
  thread::ImpersonateSession session = engine_->get_thread_pool().impersonate(&task);
  CHECK_ERROR(session.get_result());
  return kRetOk;
}

ErrorStack StorageManagerPimpl::create_storage(thread::Thread* context,
                    Metadata *metadata, Storage **storage, Epoch *commit_epoch) {
  *storage = nullptr;
  StorageId id = issue_next_storage_id();
  if (id >= get_max_storages()) {
    return ERROR_STACK(kErrorCodeStrTooManyStorages);
  }
  metadata->id_ = id;
  // CREATE STORAGE must be the only log in this transaction
  if (context->get_thread_log_buffer().get_offset_committed() !=
    context->get_thread_log_buffer().get_offset_tail()) {
    return ERROR_STACK(kErrorCodeStrMustSeparateXct);
  }

  const StorageName& name = metadata->name_;
  {
    std::lock_guard<std::mutex> guard(mod_lock_);
    if (storage_names_.find(name) != storage_names_.end()) {
      LOG(ERROR) << "This storage name already exists: " << name;
      return ERROR_STACK(kErrorCodeStrDuplicateStrname);
    }
  }

  StorageFactory* the_factory = nullptr;
  for (StorageFactory* factory : storage_factories_) {
    if (factory->is_right_metadata(metadata)) {
      the_factory = factory;
      break;
    }
  }

  CHECK_ERROR(engine_->get_xct_manager().begin_schema_xct(context));
  the_factory->add_create_log(metadata, context);  // write out log

  // commit invokes apply
  CHECK_ERROR(engine_->get_xct_manager().precommit_xct(context, commit_epoch));

  // to avoid mixing normal operations on the new storage in this epoch, advance epoch now.
  engine_->get_xct_manager().advance_current_global_epoch();

  *storage = get_storage(id);
  ASSERT_ND(*storage);
  ASSERT_ND((*storage)->get_type() == the_factory->get_type());
  return kRetOk;
}

/** A task to create an array. Used from impersonate version of create_array(). */
class CreateStorageTask final : public foedus::thread::ImpersonateTask {
 public:
  CreateStorageTask(StorageManagerPimpl* pimpl,
                    Metadata *metadata, Storage **storage, Epoch *commit_epoch)
    : pimpl_(pimpl), metadata_(metadata), storage_(storage), commit_epoch_(commit_epoch) {}
  ErrorStack run(thread::Thread* context) override {
    *storage_ = nullptr;
    CHECK_ERROR(pimpl_->create_storage(context, metadata_, storage_, commit_epoch_));
    return kRetOk;
  }

 private:
  StorageManagerPimpl* const pimpl_;
  Metadata* const metadata_;
  Storage** const storage_;
  Epoch* const commit_epoch_;
};

ErrorStack StorageManagerPimpl::create_storage(
  Metadata *metadata, Storage **storage, Epoch *commit_epoch) {
  CreateStorageTask task(this, metadata, storage, commit_epoch);
  thread::ImpersonateSession session = engine_->get_thread_pool().impersonate(&task);
  CHECK_ERROR(session.get_result());
  return kRetOk;
}

ErrorStack StorageManagerPimpl::clone_all_storage_metadata(
  snapshot::SnapshotMetadata *metadata) {
  debugging::StopWatch stop_watch;
  StorageId largest_storage_id_copy = largest_storage_id_;
  assorted::memory_fence_acq_rel();
  for (StorageId id = 1; id <= largest_storage_id_copy; ++id) {
    // TODO(Hideaki): Here, we assume deleted storages are still registered with some
    // "pseudo-delete" flag, which is not implemented yet.
    // Otherwise, we can't easily treat storage deletion in an epoch in-between two snapshots.
    if (storages_[id]) {
      metadata->storage_metadata_.push_back(storages_[id]->get_metadata()->clone());
    }
  }
  stop_watch.stop();
  LOG(INFO) << "Duplicated metadata of " << metadata->storage_metadata_.size()
    << " storages (largest_storage_id_=" << largest_storage_id_copy << ") in "
    << stop_watch.elapsed_ms() << " milliseconds";
  return kRetOk;
}

}  // namespace storage
}  // namespace foedus
