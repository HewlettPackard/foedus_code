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

ErrorStack  ArrayStorage::initialize()              { return pimpl_->initialize(); }
bool        ArrayStorage::is_initialized()  const   { return pimpl_->is_initialized(); }
ErrorStack  ArrayStorage::uninitialize()            { return pimpl_->uninitialize(); }
StorageId   ArrayStorage::get_storage_id()  const   { return pimpl_->storage_id_; }

uint16_t ArrayStorage::get_payload_size()   const   { return pimpl_->payload_size_; }
ArrayOffset ArrayStorage::get_array_size()  const   { return pimpl_->array_size_; }

ErrorStack  ArrayStorage::get_record(ArrayOffset offset, void *payload) {
    return pimpl_->get_record(offset, payload);
}
ErrorStack  ArrayStorage::get_record_part(ArrayOffset offset, void *payload,
                            uint16_t payload_offset, uint16_t payload_count) {
    return pimpl_->get_record_part(offset, payload, payload_offset, payload_count);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
