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
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/meta_log_buffer.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/engine_memory.hpp"
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
  for (uint32_t id = 1; id <= control_block_->largest_storage_id_; ++id) {
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
      block->root_page_pointer_.volatile_pointer_.clear();

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

ErrorStack StorageManager::reinitialize_for_recovered_snapshot() {
  return pimpl_->reinitialize_for_recovered_snapshot();
}
ErrorStack StorageManagerPimpl::reinitialize_for_recovered_snapshot() {
  LOG(INFO) << "Replacing existing, stale volatile root pages with the new root snapshot pages..";
  auto* memory_manager = engine_->get_memory_manager();
  const auto& resolver = memory_manager->get_global_volatile_page_resolver();
  debugging::StopWatch stop_watch;
  uint32_t refreshed_storages = 0;

  cache::SnapshotFileSet fileset(engine_);
  CHECK_ERROR(fileset.initialize());
  UninitializeGuard fileset_guard(&fileset, UninitializeGuard::kWarnIfUninitializeError);
  for (uint32_t id = 1; id <= control_block_->largest_storage_id_; ++id) {
    StorageControlBlock* block = storages_ + id;
    const SnapshotPagePointer snapshot_page_id = block->root_page_pointer_.snapshot_pointer_;
    const VolatilePagePointer volatile_page_id = block->root_page_pointer_.volatile_pointer_;
    if (!block->exists()) {
      continue;
    } else if (snapshot_page_id == 0) {
      continue;
    } else if (volatile_page_id.is_null()) {
      // some storage type allows null volatile root pages. In that case,
      // we don't have to do anything at this point. When the initial non-read-ony request
      // is made, the root volatile page will be automatically created.
      continue;
    }

    LOG(INFO) << "Re-initializing root of storage-" << id << " from the recovered snapshot."
      << "Volatile page ID=" << volatile_page_id << ", Snapshot page ID=" << snapshot_page_id;
    Page* volatile_page = resolver.resolve_offset(volatile_page_id);
    ASSERT_ND(!volatile_page->get_header().snapshot_);
    ASSERT_ND(volatile_page->get_header().storage_id_ == id);
    ASSERT_ND(volatile_page->get_volatile_page_id() == volatile_page_id);

    // Here, we assume that the initially-allocated volatile root page does NOT have
    // any child volatile page (it shouldn't!). Otherwise, the following overwrite
    // will cause leaked volatile pages.
    WRAP_ERROR_CODE(fileset.read_page(snapshot_page_id, volatile_page));
    ASSERT_ND(volatile_page->get_header().snapshot_);
    ASSERT_ND(volatile_page->get_header().storage_id_ == id);
    ASSERT_ND(volatile_page->get_snapshot_page_id() == snapshot_page_id);
    volatile_page->get_header().snapshot_ = false;
    volatile_page->get_header().page_id_ = volatile_page_id.word;
    ++refreshed_storages;
  }

  CHECK_ERROR(fileset.uninitialize());
  stop_watch.stop();
  LOG(INFO) << "Refreshed " << refreshed_storages
    << " storages with the recovered snapshot in " << stop_watch.elapsed_ms() << " milliseconds";
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
    // drop all existing storages just for releasing memories.
    // this is not a real drop, so we just invoke drop_apply
    uint32_t dropped = 0;
    for (storage::StorageId i = 1; i <= control_block_->largest_storage_id_; ++i) {
      if (storages_[i].exists()) {
        // TASK(Hideaki) we should have a separate method for this once we add "marked-for-death"
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
  // TASK(Hideaki) so far sequential search
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
  // TASK(Hideaki) so far sequential search
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
  StorageType type = block->meta_.type_;
  if (type == kArrayStorage) {
    CHECK_ERROR(array::ArrayStorage(engine_, block).drop());
  } else if (type == kHashStorage) {
    CHECK_ERROR(hash::HashStorage(engine_, block).drop());
  } else if (type == kMasstreeStorage) {
    CHECK_ERROR(masstree::MasstreeStorage(engine_, block).drop());
  } else if (type == kSequentialStorage) {
    CHECK_ERROR(sequential::SequentialStorage(engine_, block).drop());
  } else {
    LOG(FATAL) << "WTF:" << type;
  }

  char log_buffer[1 << 12];
  std::memset(log_buffer, 0, sizeof(log_buffer));
  DropLogType* drop_log = reinterpret_cast<DropLogType*>(log_buffer);
  drop_log->populate(id);
  engine_->get_log_manager()->get_meta_buffer()->commit(drop_log, commit_epoch);

  ASSERT_ND(commit_epoch->is_valid());
  block->status_ = kMarkedForDeath;
  ASSERT_ND(!block->exists());
  block->uninitialize();
  LOG(INFO) << "Dropped storage " << id << "(" << name << ")";
  return kRetOk;
}

void StorageManagerPimpl::drop_storage_apply(StorageId id) {
  StorageControlBlock* block = storages_ + id;
  ASSERT_ND(block->exists());
  StorageType type = block->meta_.type_;
  if (type == kArrayStorage) {
    COERCE_ERROR(array::ArrayStorage(engine_, block).drop());
  } else if (type == kHashStorage) {
    COERCE_ERROR(hash::HashStorage(engine_, block).drop());
  } else if (type == kMasstreeStorage) {
    COERCE_ERROR(masstree::MasstreeStorage(engine_, block).drop());
  } else if (type == kSequentialStorage) {
    COERCE_ERROR(sequential::SequentialStorage(engine_, block).drop());
  } else {
    LOG(FATAL) << "WTF:" << type;
  }
  block->status_ = kMarkedForDeath;
  ASSERT_ND(!block->exists());
  block->uninitialize();
}

template <typename STORAGE>
ErrorStack StorageManagerPimpl::create_storage_and_log(const Metadata* meta, Epoch *commit_epoch) {
  typedef typename STORAGE::ThisMetadata TheMetadata;
  typedef typename STORAGE::ThisCreateLogType TheLogType;

  StorageId id = meta->id_;
  StorageControlBlock* block = storages_ + id;
  STORAGE storage(engine_, block);
  const TheMetadata* casted_meta = reinterpret_cast< const TheMetadata *>(meta);
  ASSERT_ND(!block->exists());
  CHECK_ERROR(storage.create(*casted_meta));
  ASSERT_ND(block->exists());

  if (commit_epoch) {
    // if commit_epoch is null, it means "apply-only" mode in restart. do not log then
    char log_buffer[sizeof(TheLogType)];
    std::memset(log_buffer, 0, sizeof(log_buffer));
    TheLogType* create_log = reinterpret_cast<TheLogType*>(log_buffer);
    create_log->header_.storage_id_ = id;
    create_log->header_.log_type_code_ = log::get_log_code<TheLogType>();
    create_log->header_.log_length_ = sizeof(TheLogType);
    create_log->metadata_ = *casted_meta;
    ASSERT_ND(create_log->header_.storage_id_ == id);
    ASSERT_ND(create_log->metadata_.id_ == id);
    ASSERT_ND(create_log->metadata_.type_ == meta->type_);
    ASSERT_ND(create_log->metadata_.name_ == meta->name_);

    engine_->get_log_manager()->get_meta_buffer()->commit(create_log, commit_epoch);
  }
  return kRetOk;
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

  metadata->name_.zero_fill_remaining();  // make valgrind overload happy.
  ErrorStack result;
  if (metadata->type_ == kArrayStorage) {
    result = create_storage_and_log< array::ArrayStorage >(metadata, commit_epoch);
  } else if (metadata->type_ == kHashStorage) {
    result = create_storage_and_log< hash::HashStorage >(metadata, commit_epoch);
  } else if (metadata->type_ == kMasstreeStorage) {
    result = create_storage_and_log< masstree::MasstreeStorage >(metadata, commit_epoch);
  } else if (metadata->type_ == kSequentialStorage) {
    result = create_storage_and_log< sequential::SequentialStorage >(metadata, commit_epoch);
  } else {
    LOG(FATAL) << "WTF:" << metadata->type_;
  }
  CHECK_ERROR(result);

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
  ErrorStack result;
  if (metadata.type_ == kArrayStorage) {
    result = create_storage_and_log< array::ArrayStorage >(&metadata, nullptr);
  } else if (metadata.type_ == kHashStorage) {
    result = create_storage_and_log< hash::HashStorage >(&metadata, nullptr);
  } else if (metadata.type_ == kMasstreeStorage) {
    result = create_storage_and_log< masstree::MasstreeStorage >(&metadata, nullptr);
  } else if (metadata.type_ == kSequentialStorage) {
    result = create_storage_and_log< sequential::SequentialStorage >(&metadata, nullptr);
  } else {
    LOG(FATAL) << "WTF:" << metadata.type_;
  }
  if (result.is_error()) {
    LOG(FATAL) << "create_storage_apply() failed. " << result
      << " Failed to restart the engine";
  }

  ASSERT_ND(get_storage(id)->exists());
}

ErrorStack StorageManagerPimpl::hcc_reset_all_temperature_stat(StorageId storage_id) {
  StorageControlBlock* block = storages_ + storage_id;
  ASSERT_ND(block->exists());
  StorageType type = block->meta_.type_;
  if (type == kMasstreeStorage) {
    CHECK_ERROR(masstree::MasstreeStorage(engine_, block).hcc_reset_all_temperature_stat());
  } else if (type == kHashStorage) {
    CHECK_ERROR(hash::HashStorage(engine_, block).hcc_reset_all_temperature_stat());
  } else if (type == kArrayStorage) {
    CHECK_ERROR(array::ArrayStorage(engine_, block).hcc_reset_all_temperature_stat());
  } else {
    // Seq storage doesn't need it.
    LOG(WARNING) << "This storage type doesn't need HCC counter reset. type=" << type;
  }
  return kRetOk;
}

// define here to allow inline
xct::TrackMovedRecordResult StorageManager::track_moved_record(
  StorageId storage_id,
  xct::RwLockableXctId* old_address,
  xct::WriteXctAccess* write_set) {
  return pimpl_->track_moved_record(storage_id, old_address, write_set);
}

xct::TrackMovedRecordResult StorageManagerPimpl::track_moved_record(
  StorageId storage_id,
  xct::RwLockableXctId* old_address,
  xct::WriteXctAccess* write_set) {
  // so far Masstree and Hash have tracking
  StorageControlBlock* block = storages_ + storage_id;
  ASSERT_ND(block->exists());
  StorageType type = block->meta_.type_;
  if (type == kMasstreeStorage) {
    return masstree::MasstreeStorage(engine_, block).track_moved_record(old_address, write_set);
  } else if (type == kHashStorage) {
    return hash::HashStorage(engine_, block).track_moved_record(old_address, write_set);
  } else {
    LOG(ERROR) << "Unexpected storage type for a moved-record. Bug? type=" << type;
    return xct::TrackMovedRecordResult();
  }
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
