/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/storage/storage_manager_pimpl.hpp>
#include <foedus/storage/storage_options.hpp>
#include <foedus/storage/storage.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <glog/logging.h>
#include <atomic>
#include <cstring>
namespace foedus {
namespace storage {
StorageManagerPimpl::StorageManagerPimpl(Engine* engine)
    : engine_(engine), largest_storage_id_(0) {
    storages_ = nullptr;
    storages_capacity_ = 0;
}
ErrorStack StorageManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing StorageManager..";
    if (!engine_->get_thread_pool().is_initialized()
        || !engine_->get_log_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }

    const size_t INITIAL_CAPACITY = 1 << 12;
    storages_ = new Storage*[INITIAL_CAPACITY];
    if (!storages_) {
        return ERROR_STACK(ERROR_CODE_OUTOFMEMORY);
    }
    std::memset(storages_, 0, sizeof(Storage*) * INITIAL_CAPACITY);
    storages_capacity_ = INITIAL_CAPACITY;

    return RET_OK;
}

ErrorStack StorageManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing StorageManager..";
    ErrorStackBatch batch;
    if (!engine_->get_thread_pool().is_initialized()
        || !engine_->get_log_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }

    delete[] storages_;
    storages_ = nullptr;
    return RET_OK;
}

StorageId StorageManagerPimpl::issue_next_storage_id() {
    std::lock_guard<std::mutex> guard(mod_lock_);  // implies fence too
    ++largest_storage_id_;
    LOG(INFO) << "Incremented largest_storage_id_: " << largest_storage_id_;
    return largest_storage_id_;
}

Storage* StorageManagerPimpl::get_storage(StorageId id) {
    std::atomic_thread_fence(std::memory_order_acquire);  // to sync with expand
    return storages_[id];
}


ErrorStack StorageManagerPimpl::register_storage(Storage* storage) {
    assert(storage);
    assert(storage->is_initialized());
    StorageId id = storage->get_id();
    LOG(INFO) << "Adding storage of ID-" << id << "(" << storage->get_name() << ")";
    if (storages_capacity_ <= id) {
        CHECK_ERROR(expand_storage_array(id));
    }

    assert(storages_capacity_ > id);
    std::lock_guard<std::mutex> guard(mod_lock_);
    if (storages_[id]) {
        LOG(ERROR) << "Duplicate register_storage() call? ID=" << id;
        return ERROR_STACK(ERROR_CODE_STR_DUPLICATE_STRID);
    }
    storages_[id] = storage;
    if (id > largest_storage_id_) {
        largest_storage_id_ = id;
    }
    return RET_OK;
}

ErrorStack StorageManagerPimpl::remove_storage(StorageId id) {
    std::lock_guard<std::mutex> guard(mod_lock_);
    if (storages_[id]) {
        LOG(INFO) << "Removing storage of ID" << id;
        ErrorStack error_stack = storages_[id]->uninitialize();
        storages_[id] = nullptr;
        return error_stack;
    } else {
        LOG(WARNING) << "No storage of ID=" << id << ". nothing to delete";
        return RET_OK;
    }
}


ErrorStack StorageManagerPimpl::expand_storage_array(StorageId new_size) {
    LOG(INFO) << "Expanding storages_. new_size=" << new_size;
    std::lock_guard<std::mutex> guard(mod_lock_);
    if (new_size <= storages_capacity_) {
        LOG(INFO) << "Someone else has expanded?";
        return RET_OK;
    }

    new_size = (new_size + 1) * 2;  // 2x margin to avoid frequent expansion.
    Storage** new_array = new Storage*[new_size];
    if (!new_array) {
        return ERROR_STACK(ERROR_CODE_OUTOFMEMORY);
    }

    // copy and switch (fence to prevent compiler from doing something stupid)
    std::memcpy(new_array, storages_, sizeof(Storage*) * storages_capacity_);
    std::memset(new_array + storages_capacity_, 0,
                    sizeof(Storage*) * (new_size - storages_capacity_));
    // we must announce the new storages_ to read-threads first because
    // new_size > storages_capacity_.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    storages_ = new_array;
    storages_capacity_ = new_size;
    return RET_OK;
}


}  // namespace storage
}  // namespace foedus
