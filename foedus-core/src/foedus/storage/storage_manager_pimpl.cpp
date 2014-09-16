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
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/storage_options.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
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

  // attach shared memories
  soc::GlobalMemoryAnchors* anchors
    = engine_->get_soc_manager().get_shared_memory_repo()->get_global_memory_anchors();
  control_block_ = anchors->storage_manager_memory_;
  storages_ = anchors->storage_memories_;
  storage_name_sort_ = anchors->storage_name_sort_memory_;

  if (engine_->is_master()) {
    // initialize the shared memory. only on master engine
    control_block_->initialize();
    control_block_->largest_storage_id_ = 0;
  }
  return kRetOk;
}

ErrorStack StorageManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing StorageManager..";
  ErrorStackBatch batch;
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_log_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  if (engine_->is_master()) {
    control_block_->uninitialize();
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}

StorageId StorageManagerPimpl::issue_next_storage_id() {
  soc::SharedMutexScope guard(&control_block_->mod_lock_);  // implies fence too
  ++control_block_->largest_storage_id_;
  LOG(INFO) << "Incremented largest_storage_id_: " << control_block_->largest_storage_id_;
  return control_block_->largest_storage_id_;
}

StorageControlBlock* StorageManagerPimpl::get_storage(const StorageName& name) {
  soc::SharedMutexScope guard(&control_block_->mod_lock_);
  // TODO(Hideaki) so far sequential search
  for (uint32_t i = 0; i <= control_block_->largest_storage_id_; ++i) {
    if (storages_[i].meta_.name_ == name) {
      return &storages_[i];
    }
  }
  LOG(WARNING) << "Requested storage name '" << name << "' was not found";
  return &storages_[0];  // storage ID 0 is always not-initialized
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
  if (get_storage(id)) {
    LOG(ERROR) << "Duplicate register_storage() call? ID=" << id;
    return ERROR_STACK(kErrorCodeStrDuplicateStrid);
  }
  if (get_storage(storage->get_name())) {
    LOG(ERROR) << "Duplicate register_storage() call? Name=" << storage->get_name();
    return ERROR_STACK(kErrorCodeStrDuplicateStrname);
  }
  storages_[id] = storage;
  // TODO(Hideaki) storage name map not used yet
  // storage_names_.insert(std::pair< StorageName, Storage* >(storage->get_name(), storage));
  ASSERT_ND(id <= control_block_->largest_storage_id_);
  return kRetOk;
}

ErrorStack StorageManagerPimpl::drop_storage(StorageId id, Epoch *commit_epoch) {
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

  storages_[id] = nullptr;
  delete storage;
  LOG(INFO) << "Droped storage " << id << "(" << name << ")";
}

ErrorStack StorageManagerPimpl::create_storage(Metadata *metadata, Epoch *commit_epoch) {
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
  if (get_storage(name)) {
    LOG(ERROR) << "This storage name already exists: " << name;
    return ERROR_STACK(kErrorCodeStrDuplicateStrname);
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

ErrorStack StorageManagerPimpl::clone_all_storage_metadata(
  snapshot::SnapshotMetadata *metadata) {
  debugging::StopWatch stop_watch;
  StorageId largest_storage_id_copy = control_block_->largest_storage_id_;
  assorted::memory_fence_acq_rel();
  for (StorageId id = 1; id <= largest_storage_id_copy; ++id) {
    // TODO(Hideaki): Here, we assume deleted storages are still registered with some
    // "pseudo-delete" flag, which is not implemented yet.
    // Otherwise, we can't easily treat storage deletion in an epoch in-between two snapshots.
    if (storages_[id].exists()) {
      // TODO(Hideaki) not implemented
      // metadata->storage_metadata_.push_back(storages_[id].meta_->clone());
    }
  }
  stop_watch.stop();
  /*
  LOG(INFO) << "Duplicated metadata of " << metadata->storage_metadata_.size()
    << " storages (largest_storage_id_=" << largest_storage_id_copy << ") in "
    << stop_watch.elapsed_ms() << " milliseconds";
    */
  return kRetOk;
}

}  // namespace storage
}  // namespace foedus
