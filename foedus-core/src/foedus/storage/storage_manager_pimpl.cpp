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
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/snapshot/snapshot_metadata.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/partitioner.hpp"
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
  if (!engine_->get_thread_pool()->is_initialized()
    || !engine_->get_log_manager()->is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }

  // attach shared memories
  soc::GlobalMemoryAnchors* anchors
    = engine_->get_soc_manager()->get_shared_memory_repo()->get_global_memory_anchors();
  control_block_ = anchors->storage_manager_memory_;
  storages_ = anchors->storage_memories_;
  storage_name_sort_ = anchors->storage_name_sort_memory_;

  if (engine_->is_master()) {
    // initialize the shared memory. only on master engine
    control_block_->initialize();
    control_block_->largest_storage_id_ = 0;

    // also initialize the shared memory for partitioner
    uint32_t max_storages = get_max_storages();
    for (storage::StorageId i = 0; i < max_storages; ++i) {
      anchors->partitioner_metadata_[i].initialize();
    }
    // set the size of partitioner data
    anchors->partitioner_metadata_[0].data_size_
      = engine_->get_options().storage_.partitioner_data_memory_mb_ * (1ULL << 20);
    // Then, initialize storages with latest snapshot
    CHECK_ERROR(initialize_read_latest_snapshot());
  }
  return kRetOk;
}

ErrorStack StorageManagerPimpl::initialize_read_latest_snapshot() {
  ASSERT_ND(engine_->is_master());
  LOG(INFO) << "Initializing list of storages with latest snapshot file...";
  snapshot::SnapshotId snapshot_id = engine_->get_savepoint_manager()->get_latest_snapshot_id();
  if (snapshot_id == snapshot::kNullSnapshotId) {
    LOG(INFO) << "There was no previous snapshot. start from empty storages";
    return kRetOk;
  }

  snapshot::SnapshotMetadata metadata;
  CHECK_ERROR(engine_->get_snapshot_manager()->read_snapshot_metadata(snapshot_id, &metadata));
  LOG(INFO) << "Latest snapshot contains " << metadata.largest_storage_id_ << " storages";
  control_block_->largest_storage_id_ = metadata.largest_storage_id_;

  debugging::StopWatch stop_watch;
  uint32_t active_storages = 0;
  for (uint32_t id = 1; id < control_block_->largest_storage_id_; ++id) {
    StorageControlBlock* block = storages_ + id;
    const StorageControlBlock& snapshot_block = metadata.storage_control_blocks_[id];
    if (snapshot_block.status_ != kExists) {
      VLOG(0) << "Storage-" << id << " is a dropped storage.";
      block->status_ = kNotExists;
    } else {
      VLOG(0) << "Storage-" << id << " exists in the snapshot.";
      ASSERT_ND(snapshot_block.meta_.id_ == id);
      ASSERT_ND(!snapshot_block.meta_.name_.empty());
      ASSERT_ND(!exists(snapshot_block.meta_.name_));
      block->initialize();
      block->root_page_pointer_.snapshot_pointer_ = snapshot_block.meta_.root_snapshot_page_id_;
      block->root_page_pointer_.volatile_pointer_.components.offset = 0;

      block->meta_.type_ = snapshot_block.meta_.type_;
      switch (snapshot_block.meta_.type_) {
      case kArrayStorage:
        CHECK_ERROR(array::ArrayStorage(engine_, block).load(snapshot_block));
        break;
      case kHashStorage:
        CHECK_ERROR(hash::HashStorage(engine_, block).load(snapshot_block));
        break;
      case kMasstreeStorage:
        CHECK_ERROR(masstree::MasstreeStorage(engine_, block).load(snapshot_block));
        break;
      case kSequentialStorage:
        CHECK_ERROR(sequential::SequentialStorage(engine_, block).load(snapshot_block));
        break;
      default:
        return ERROR_STACK(kErrorCodeStrUnsupportedMetadata);
      }

      ASSERT_ND(get_storage(id)->exists());

      ++active_storages;
    }
  }
  stop_watch.stop();
  LOG(INFO) << "Found " << active_storages
    << " active storages  in " << stop_watch.elapsed_ms() << " milliseconds";
  return kRetOk;
}

ErrorStack StorageManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing StorageManager..";
  ErrorStackBatch batch;
  if (!engine_->get_thread_pool()->is_initialized()
    || !engine_->get_log_manager()->is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  if (engine_->is_master()) {
    // also uninitialize the shared memory for partitioner
    soc::GlobalMemoryAnchors* anchors
      = engine_->get_soc_manager()->get_shared_memory_repo()->get_global_memory_anchors();
    uint32_t max_storages = get_max_storages();
    for (storage::StorageId i = 0; i < max_storages; ++i) {
      anchors->partitioner_metadata_[i].uninitialize();
    }

    // drop all existing storages just for releasing memories.
    // this is not a real drop, so we just invoke drop_apply
    uint32_t dropped = 0;
    for (storage::StorageId i = 1; i <= control_block_->largest_storage_id_; ++i) {
      if (storages_[i].exists()) {
        // TODO(Hideaki) we should have a separate method for this once we add "marked-for-death"
        // feature.
        drop_storage_apply(i);
        ++dropped;
        ASSERT_ND(!storages_[i].exists());
      }
    }
    LOG(INFO) << "Uninitialized " << dropped << " storages";

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
  std::memset(log_buffer, 0, sizeof(log_buffer));
  DropLogType* drop_log = reinterpret_cast<DropLogType*>(log_buffer);
  drop_log->populate(id);
  engine_->get_log_manager()->get_meta_buffer()->commit(drop_log, commit_epoch);

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
  ASSERT_ND(block->exists());
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

template <typename CREATE_LOG, typename METADATA>
void construct_create_log(const Metadata& meta, void* buffer) {
  CREATE_LOG* create_log = reinterpret_cast<CREATE_LOG*>(buffer);
  create_log->header_.storage_id_ = meta.id_;
  create_log->header_.log_type_code_ = log::get_log_code<CREATE_LOG>();
  create_log->header_.log_length_ = sizeof(CREATE_LOG);
  create_log->metadata_ = reinterpret_cast<const METADATA&>(meta);
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
  std::memset(log_buffer, 0, sizeof(log_buffer));
  metadata->name_.zero_fill_remaining();  // make valgrind overload happy.
  if (metadata->type_ == kArrayStorage) {
    construct_create_log< array::ArrayCreateLogType, array::ArrayMetadata >(*metadata, log_buffer);
  } else if (metadata->type_ == kHashStorage) {
    construct_create_log< hash::HashCreateLogType, hash::HashMetadata >(*metadata, log_buffer);
  } else if (metadata->type_ == kMasstreeStorage) {
    construct_create_log< masstree::MasstreeCreateLogType, masstree::MasstreeMetadata >(
      *metadata,
      log_buffer);
  } else if (metadata->type_ == kSequentialStorage) {
    construct_create_log< sequential::SequentialCreateLogType, sequential::SequentialMetadata >(
      *metadata,
      log_buffer);
  } else {
    LOG(FATAL) << "WTF:" << metadata->type_;
  }

  CreateLogType* create_log = reinterpret_cast<CreateLogType*>(log_buffer);
  ASSERT_ND(create_log->header_.storage_id_ == id);
  ASSERT_ND(create_log->metadata_.id_ == id);
  ASSERT_ND(create_log->metadata_.type_ == metadata->type_);
  ASSERT_ND(create_log->metadata_.name_ == name);
  engine_->get_log_manager()->get_meta_buffer()->commit(create_log, commit_epoch);

  ASSERT_ND(commit_epoch->is_valid());
  ASSERT_ND(get_storage(id)->exists());
  return kRetOk;
}

void StorageManagerPimpl::create_storage_apply(const Metadata& metadata) {
  // this method is called only while restart, so no race.
  ASSERT_ND(metadata.id_ > 0);
  ASSERT_ND(!metadata.name_.empty());
  StorageId id = metadata.id_;
  if (id > control_block_->largest_storage_id_) {
    control_block_->largest_storage_id_ = id;
  }

  ASSERT_ND(!exists(metadata.name_));

  get_storage(id)->initialize();
  ASSERT_ND(!get_storage(id)->exists());
  storages_[id].meta_.type_ = metadata.type_;
  ErrorStack create_error = storage_pseudo_polymorph(
    engine_,
    storages_ + id,
    [metadata](Storage* obj){ return obj->create(metadata); });
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
