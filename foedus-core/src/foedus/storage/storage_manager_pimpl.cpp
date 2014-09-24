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
#include "foedus/log/meta_log_buffer.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/snapshot/snapshot_metadata.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/storage_options.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {

template <typename HANDLER>
ErrorStack storage_pseudo_polymorph(Engine* engine, StorageControlBlock* block, HANDLER handler) {
  StorageType type = block->meta_.type_;
  if (type == kArrayStorage) {
    array::ArrayStorage obj(engine, block);
    CHECK_ERROR(handler(&obj));
  } else if (type == kHashStorage) {
    hash::HashStorage obj(engine, block);
    CHECK_ERROR(handler(&obj));
  } else if (type == kMasstreeStorage) {
    masstree::MasstreeStorage obj(engine, block);
    CHECK_ERROR(handler(&obj));
  } else if (type == kSequentialStorage) {
    sequential::SequentialStorage obj(engine, block);
    CHECK_ERROR(handler(&obj));
  } else {
    return ERROR_STACK(kErrorCodeStrUnsupportedMetadata);
  }
  return kRetOk;
}

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
bool StorageManagerPimpl::exists(const StorageName& name) {
  soc::SharedMutexScope guard(&control_block_->mod_lock_);
  // TODO(Hideaki) so far sequential search
  for (uint32_t i = 0; i <= control_block_->largest_storage_id_; ++i) {
    if (storages_[i].meta_.name_ == name) {
      return true;
    }
  }
  return false;
}

ErrorStack StorageManagerPimpl::drop_storage(StorageId id, Epoch *commit_epoch) {
  StorageControlBlock* block = storages_ + id;
  if (!block->exists()) {
    LOG(ERROR) << "This storage ID does not exist or has been already dropped: " << id;
    return ERROR_STACK(kErrorCodeStrAlreadyDropped);
  }

  StorageName name = block->meta_.name_;
  LOG(INFO) << "Dropping storage " << id << "(" << name << ")";
  ErrorStack drop_error = storage_pseudo_polymorph(
    engine_,
    block,
    [](Storage* obj){ return obj->drop(); });
  if (drop_error.is_error()) {
    LOG(ERROR) << "Failed to drop storage " << id << "(" << name << ")";
    return drop_error;
  }

  char log_buffer[1 << 12];
  DropLogType* drop_log = reinterpret_cast<DropLogType*>(log_buffer);
  drop_log->populate(id);
  engine_->get_log_manager().get_meta_buffer()->commit(drop_log, commit_epoch);

  ASSERT_ND(commit_epoch->is_valid());
  block->status_ = kDropped;
  ASSERT_ND(!block->exists());
  block->uninitialize();
  LOG(INFO) << "Dropped storage " << id << "(" << name << ")";
  return kRetOk;
}

void StorageManagerPimpl::drop_storage_apply(StorageId id) {
  // this method is called only while restart, so no race.
  StorageControlBlock* block = storages_ + id;
  ASSERT_ND(!block->exists());
  ErrorStack drop_error = storage_pseudo_polymorph(
    engine_,
    block,
    [](Storage* obj){ return obj->drop(); });
  if (drop_error.is_error()) {
    LOG(FATAL) << "drop_storage_apply() failed. " << drop_error
      << " Failed to restart the engine";
  }
  block->status_ = kDropped;
  ASSERT_ND(!block->exists());
  block->uninitialize();
}

void construct_create_log(Metadata* meta, void* buffer) {
  StorageType type = meta->type_;
  if (type == kArrayStorage) {
    array::ArrayCreateLogType::construct(meta, buffer);
  } else if (type == kHashStorage) {
    hash::HashCreateLogType::construct(meta, buffer);
  } else if (type == kMasstreeStorage) {
    masstree::MasstreeCreateLogType::construct(meta, buffer);
  } else if (type == kSequentialStorage) {
    sequential::SequentialCreateLogType::construct(meta, buffer);
  } else {
    LOG(FATAL) << "WTF:" << type;
  }
}

ErrorStack StorageManagerPimpl::create_storage(Metadata *metadata, Epoch *commit_epoch) {
  *commit_epoch = INVALID_EPOCH;
  StorageId id = issue_next_storage_id();
  if (id >= get_max_storages()) {
    return ERROR_STACK(kErrorCodeStrTooManyStorages);
  }
  if (metadata->name_.empty()) {
    return ERROR_STACK(kErrorCodeStrEmptyName);
  }
  metadata->id_ = id;

  const StorageName& name = metadata->name_;
  if (exists(name)) {
    LOG(ERROR) << "This storage name already exists: " << name;
    return ERROR_STACK(kErrorCodeStrDuplicateStrname);
  }

  get_storage(id)->initialize();
  ASSERT_ND(!get_storage(id)->exists());
  storages_[id].meta_.type_ = metadata->type_;
  ErrorStack create_error = storage_pseudo_polymorph(
    engine_,
    storages_ + id,
    [metadata](Storage* obj){ return obj->create(*metadata); });
  CHECK_ERROR(create_error);

  char log_buffer[1 << 12];
  construct_create_log(metadata, log_buffer);
  log::StorageLogType* create_log = reinterpret_cast<log::StorageLogType*>(log_buffer);
  engine_->get_log_manager().get_meta_buffer()->commit(create_log, commit_epoch);

  ASSERT_ND(commit_epoch->is_valid());
  ASSERT_ND(get_storage(id)->exists());
  return kRetOk;
}

void StorageManagerPimpl::create_storage_apply(Metadata *metadata) {
  // this method is called only while restart, so no race.
  ASSERT_ND(metadata->id_ > 0);
  ASSERT_ND(!metadata->name_.empty());
  StorageId id = metadata->id_;
  if (id > control_block_->largest_storage_id_) {
    control_block_->largest_storage_id_ = id;
  }

  ASSERT_ND(!exists(metadata->name_));

  get_storage(id)->initialize();
  ASSERT_ND(!get_storage(id)->exists());
  storages_[id].meta_.type_ = metadata->type_;
  ErrorStack create_error = storage_pseudo_polymorph(
    engine_,
    storages_ + id,
    [metadata](Storage* obj){ return obj->create(*metadata); });
  if (create_error.is_error()) {
    LOG(FATAL) << "create_storage_apply() failed. " << create_error
      << " Failed to restart the engine";
  }

  ASSERT_ND(get_storage(id)->exists());
}

bool StorageManagerPimpl::track_moved_record(StorageId storage_id, xct::WriteXctAccess* write) {
  // so far only Masstree has tracking
  ASSERT_ND(storages_[storage_id].exists());
  ASSERT_ND(storages_[storage_id].meta_.type_ == kMasstreeStorage);
  return masstree::MasstreeStorage(engine_, storages_ + storage_id).track_moved_record(write);
}

xct::LockableXctId* StorageManagerPimpl::track_moved_record(
  StorageId storage_id,
  xct::LockableXctId* address) {
  ASSERT_ND(storages_[storage_id].exists());
  ASSERT_ND(storages_[storage_id].meta_.type_ == kMasstreeStorage);
  return masstree::MasstreeStorage(engine_, storages_ + storage_id).track_moved_record(address);
}

ErrorStack StorageManagerPimpl::clone_all_storage_metadata(
  snapshot::SnapshotMetadata *metadata) {
  debugging::StopWatch stop_watch;
  metadata->largest_storage_id_ = control_block_->largest_storage_id_;
  assorted::memory_fence_acq_rel();

  // not just the metadata, just copy the whole control block.
  // this is a single memcpy, which should be much more efficient.
  uint64_t memory_size
    = static_cast<uint64_t>(metadata->largest_storage_id_ + 1)
      * soc::GlobalMemoryAnchors::kStorageMemorySize;
  metadata->storage_control_blocks_memory_.alloc(
    memory_size,
    1 << 12,
    memory::AlignedMemory::kNumaAllocOnnode,
    0);
  metadata->storage_control_blocks_ = reinterpret_cast<storage::StorageControlBlock*>(
    metadata->storage_control_blocks_memory_.get_block());
  std::memcpy(metadata->storage_control_blocks_, storages_, memory_size);

  stop_watch.stop();
  LOG(INFO) << "Duplicated metadata of " << metadata->largest_storage_id_
    << " storages  in " << stop_watch.elapsed_ms() << " milliseconds";
  return kRetOk;
}

}  // namespace storage
}  // namespace foedus
