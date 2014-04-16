/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/storage_manager_pimpl.hpp>
namespace foedus {
namespace storage {
StorageManager::StorageManager(Engine* engine) {
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
StorageId StorageManager::issue_next_storage_id() { return pimpl_->issue_next_storage_id(); }
ErrorStack StorageManager::register_storage(Storage* storage) {
    return pimpl_->register_storage(storage);
}
ErrorStack StorageManager::remove_storage(StorageId id) { return pimpl_->remove_storage(id); }

}  // namespace storage
}  // namespace foedus
