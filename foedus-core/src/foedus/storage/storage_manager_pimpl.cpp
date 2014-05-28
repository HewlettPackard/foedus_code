/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/storage/storage_manager_pimpl.hpp>
#include <foedus/storage/storage_options.hpp>
#include <foedus/storage/storage.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <glog/logging.h>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
namespace foedus {
namespace storage {
ErrorStack StorageManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing StorageManager..";
    if (!engine_->get_thread_pool().is_initialized()
        || !engine_->get_log_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }

    largest_storage_id_ = 0;
    storages_ = nullptr;
    storages_capacity_ = 0;

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
    return RET_OK;
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
        return ERROR_STACK(ERROR_CODE_STR_DUPLICATE_STRID);
    }
    if (storage_names_.find(storage->get_name()) != storage_names_.end()) {
        LOG(ERROR) << "Duplicate register_storage() call? Name=" << storage->get_name();
        return ERROR_STACK(ERROR_CODE_STR_DUPLICATE_STRNAME);
    }
    storages_[id] = storage;
    storage_names_.insert(std::pair< std::string, Storage* >(storage->get_name(), storage));
    if (id > largest_storage_id_) {
        largest_storage_id_ = id;
    }
    return RET_OK;
}

ErrorStack StorageManagerPimpl::remove_storage(StorageId id) {
    std::lock_guard<std::mutex> guard(mod_lock_);
    if (storages_[id]) {
        LOG(INFO) << "Removing storage of ID" << id;
        std::string name = storages_[id]->get_name();
        storage_names_.erase(name);
        // TODO(Hideaki) should separate this heavy stuff out of critical section.
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
    assorted::memory_fence_release();
    storages_ = new_array;
    assorted::memory_fence_release();
    storages_capacity_ = new_size;
    return RET_OK;
}

ErrorStack StorageManagerPimpl::create_array(thread::Thread* context, const std::string& name,
            uint16_t payload_size, array::ArrayOffset array_size, array::ArrayStorage** out) {
    *out = nullptr;
    StorageId id = issue_next_storage_id();
    std::unique_ptr<array::ArrayStorage> array(new array::ArrayStorage
        (engine_, id, name, payload_size, array_size, DualPagePointer(), true));
    CHECK_ERROR(array->initialize());
    CHECK_ERROR(array->create(context));
    *out = array.release();  // No error, so take over the ownership from unique_ptr.
    return RET_OK;
}

/** A task to create an array. Used from create_array_impersonate(). */
class CreateArrayTask final : public foedus::thread::ImpersonateTask {
 public:
    CreateArrayTask(StorageManagerPimpl* pimpl, const std::string& name, uint16_t payload,
                    array::ArrayOffset array_size, array::ArrayStorage** out)
        : pimpl_(pimpl), name_(name), payload_(payload), array_size_(array_size), out_(out) {}
    ErrorStack run(thread::Thread* context) override {
        *out_ = nullptr;
        CHECK_ERROR(pimpl_->create_array(context, name_, payload_, array_size_, out_));
        return RET_OK;
    }

 private:
    StorageManagerPimpl* pimpl_;
    const std::string& name_;
    const uint16_t payload_;
    const array::ArrayOffset array_size_;
    array::ArrayStorage** out_;
};

ErrorStack StorageManagerPimpl::create_array_impersonate(const std::string& name,
            uint16_t payload_size, array::ArrayOffset array_size, array::ArrayStorage** out) {
    CreateArrayTask task(this, name, payload_size, array_size, out);
    thread::ImpersonateSession session = engine_->get_thread_pool().impersonate(&task);
    CHECK_ERROR(session.get_result());
    return RET_OK;
}


}  // namespace storage
}  // namespace foedus
