/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_storage_pimpl.hpp>
namespace foedus {
namespace storage {
namespace array {
ArrayStorage::ArrayStorage(Engine* engine, StorageId storage_id) {
    pimpl_ = new ArrayStoragePimpl(engine, storage_id);
}
ArrayStorage::~ArrayStorage() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack  ArrayStorage::initialize()      { return pimpl_->initialize(); }
bool        ArrayStorage::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  ArrayStorage::uninitialize()    { return pimpl_->uninitialize(); }
StorageId   ArrayStorage::get_storage_id() const { return pimpl_->storage_id_; }


}  // namespace array
}  // namespace storage
}  // namespace foedus
