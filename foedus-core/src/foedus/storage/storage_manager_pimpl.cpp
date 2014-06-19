/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/debugging/stop_watch.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/snapshot/snapshot_metadata.hpp>
#include <foedus/storage/storage_log_types.hpp>
#include <foedus/storage/storage_manager_pimpl.hpp>
#include <foedus/storage/storage_options.hpp>
#include <foedus/storage/storage.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_log_types.hpp>
#include <foedus/storage/metadata.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <glog/logging.h>
#include <cstring>
#include <string>
#include <utility>
namespace foedus {
namespace storage {
ErrorStack StorageManagerPimpl::initialize_once() {
  LOG(INFO) << "Initializing StorageManager..";
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_log_manager().is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }

  largest_storage_id_ = 0;
  storages_ = nullptr;
  storages_capacity_ = 0;

  const size_t kInitialCapacity = 1 << 12;
  storages_ = new Storage*[kInitialCapacity];
  if (!storages_) {
    return ERROR_STACK(kErrorCodeOutofmemory);
  }
  std::memset(storages_, 0, sizeof(Storage*) * kInitialCapacity);
  storages_capacity_ = kInitialCapacity;

  return kRetOk;
}

ErrorStack StorageManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing StorageManager..";
  ErrorStackBatch batch;
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_log_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  for (size_t i = 0; i < storages_capacity_; ++i) {
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
  return SUMMARIZE_ERROR_BATCH(batch);
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
Storage* StorageManagerPimpl::get_storage(const std::string& name) {
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
  if (storages_capacity_ <= id) {
    CHECK_ERROR(expand_storage_array(id));
  }

  ASSERT_ND(storages_capacity_ > id);
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
  storage_names_.insert(std::pair< std::string, Storage* >(storage->get_name(), storage));
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
  std::string name = storage->get_name();
  LOG(INFO) << "Dropping storage " << id << "(" << name << ")";
  COERCE_ERROR(storage->uninitialize());
  LOG(INFO) << "Uninitialized storage " << id << "(" << name << ")";

  std::lock_guard<std::mutex> guard(mod_lock_);
  storage_names_.erase(name);
  storages_[id] = nullptr;
  delete storage;
  LOG(INFO) << "Droped storage " << id << "(" << name << ")";
}

/** A task to drop a task. Used from drop_storage_impersonate(). */
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

ErrorStack StorageManagerPimpl::drop_storage_impersonate(StorageId id, Epoch *commit_epoch) {
  DropStorageTask task(this, id, commit_epoch);
  thread::ImpersonateSession session = engine_->get_thread_pool().impersonate(&task);
  CHECK_ERROR(session.get_result());
  return kRetOk;
}


ErrorStack StorageManagerPimpl::expand_storage_array(StorageId new_size) {
  LOG(INFO) << "Expanding storages_. new_size=" << new_size;
  std::lock_guard<std::mutex> guard(mod_lock_);
  if (new_size <= storages_capacity_) {
    LOG(INFO) << "Someone else has expanded?";
    return kRetOk;
  }

  new_size = (new_size + 1) * 2;  // 2x margin to avoid frequent expansion.
  Storage** new_array = new Storage*[new_size];
  if (!new_array) {
    return ERROR_STACK(kErrorCodeOutofmemory);
  }

  // copy and switch (fence to prevent compiler from doing something stupid)
  std::memcpy(new_array, storages_, sizeof(Storage*) * storages_capacity_);
  std::memset(new_array + storages_capacity_, 0,
          sizeof(Storage*) * (new_size - storages_capacity_));
  // we must announce the new storages_ to read-threads first because
  // new_size > storages_capacity_.
  assorted::memory_fence_release();
  storages_ = new_array;
  assorted::memory_fence_release();
  storages_capacity_ = new_size;
  return kRetOk;
}

ErrorStack StorageManagerPimpl::create_array(thread::Thread* context, const std::string& name,
      uint16_t payload_size, array::ArrayOffset array_size, array::ArrayStorage** out,
      Epoch *commit_epoch) {
  StorageId id = issue_next_storage_id();
  // CREATE ARRAY must be the only log in this transaction
  if (context->get_thread_log_buffer().get_offset_committed() !=
    context->get_thread_log_buffer().get_offset_tail()) {
    return ERROR_STACK(kErrorCodeStrMustSeparateXct);
  }

  if (payload_size == 0) {
    // Array storage has no notion of insert/delete, thus payload=null doesn't make sense.
    LOG(INFO) << "Empty payload is not allowed for array storage";
    return ERROR_STACK(kErrorCodeStrArrayInvalidOption);
  } else if (array_size == 0) {
    LOG(INFO) << "Empty array is not allowed for array storage";
    return ERROR_STACK(kErrorCodeStrArrayInvalidOption);
  }
  {
    std::lock_guard<std::mutex> guard(mod_lock_);
    if (storage_names_.find(name) != storage_names_.end()) {
      LOG(ERROR) << "This storage name already exists: " << name;
      return ERROR_STACK(kErrorCodeStrDuplicateStrname);
    }
  }

  CHECK_ERROR(engine_->get_xct_manager().begin_schema_xct(context));

  // write out log
  uint16_t log_length = array::CreateLogType::calculate_log_length(name.size());
  array::CreateLogType* log_entry = reinterpret_cast<array::CreateLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(id, array_size, payload_size, name.size(), name.data());

  // commit invokes apply
  CHECK_ERROR(engine_->get_xct_manager().precommit_xct(context, commit_epoch));

  // to avoid mixing normal operations on the new storage in this epoch, advance epoch now.
  engine_->get_xct_manager().advance_current_global_epoch();

  Storage* new_storage = get_storage(id);
  ASSERT_ND(new_storage);
  ASSERT_ND(new_storage->get_type() == kArrayStorage);
  *out = dynamic_cast<array::ArrayStorage*>(new_storage);
  ASSERT_ND(*out);
  return kRetOk;
}

/** A task to create an array. Used from create_array_impersonate(). */
class CreateArrayTask final : public foedus::thread::ImpersonateTask {
 public:
  CreateArrayTask(StorageManagerPimpl* pimpl, const std::string& name, uint16_t payload,
          array::ArrayOffset array_size, array::ArrayStorage** out, Epoch *commit_epoch)
    : pimpl_(pimpl), name_(name), payload_(payload), array_size_(array_size), out_(out),
      commit_epoch_(commit_epoch) {}
  ErrorStack run(thread::Thread* context) override {
    *out_ = nullptr;
    CHECK_ERROR(pimpl_->create_array(
      context, name_, payload_, array_size_, out_, commit_epoch_));
    return kRetOk;
  }

 private:
  StorageManagerPimpl* pimpl_;
  const std::string& name_;
  const uint16_t payload_;
  const array::ArrayOffset array_size_;
  array::ArrayStorage** out_;
  Epoch *commit_epoch_;
};

ErrorStack StorageManagerPimpl::create_array_impersonate(const std::string& name,
  uint16_t payload_size, array::ArrayOffset array_size, array::ArrayStorage** out,
  Epoch *commit_epoch) {
  CreateArrayTask task(this, name, payload_size, array_size, out, commit_epoch);
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
