/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/storage_manager_pimpl.hpp>
#include <string>
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
ErrorStack StorageManager::drop_storage(thread::Thread* context, StorageId id) {
    return pimpl_->drop_storage(context, id);
}
ErrorStack StorageManager::drop_storage_impersonate(StorageId id) {
    return pimpl_->drop_storage_impersonate(id);
}

ErrorStack StorageManager::create_array(thread::Thread* context, const std::string& name,
        uint16_t payload_size, array::ArrayOffset array_size, array::ArrayStorage** out) {
    return pimpl_->create_array(context, name, payload_size, array_size, out);
}
ErrorStack StorageManager::create_array_impersonate(const std::string& name, uint16_t payload_size,
                                        array::ArrayOffset array_size, array::ArrayStorage** out) {
    return pimpl_->create_array_impersonate(name, payload_size, array_size, out);
}


}  // namespace storage
}  // namespace foedus
