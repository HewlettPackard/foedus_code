/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_storage_pimpl.hpp>
#include <string>
namespace foedus {
namespace storage {
namespace array {
ArrayStorage::ArrayStorage(Engine* engine, StorageId id, const std::string &name,
        uint16_t payload_size, ArrayOffset array_size, DualPagePointer root_page, bool create)
    : pimpl_(nullptr) {
    pimpl_ = new ArrayStoragePimpl(engine, this, id, name,
                                   payload_size, array_size, root_page, create);
}
ArrayStorage::~ArrayStorage() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack  ArrayStorage::initialize()              { return pimpl_->initialize(); }
ErrorStack  ArrayStorage::uninitialize()            { return pimpl_->uninitialize(); }
ErrorStack  ArrayStorage::create(thread::Thread* context)   { return pimpl_->create(context); }

bool        ArrayStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        ArrayStorage::exists()           const  { return pimpl_->exist_; }
uint16_t    ArrayStorage::get_payload_size() const  { return pimpl_->payload_size_; }
ArrayOffset ArrayStorage::get_array_size()   const  { return pimpl_->array_size_; }
StorageId   ArrayStorage::get_id()           const  { return pimpl_->id_; }
const std::string& ArrayStorage::get_name()  const  { return pimpl_->name_; }

ErrorStack ArrayStorage::get_record(thread::Thread* context, ArrayOffset offset,
                            void *payload, uint16_t payload_offset, uint16_t payload_count) {
    return pimpl_->get_record(context, offset, payload, payload_offset, payload_count);
}

ErrorStack ArrayStorage::overwrite_record(thread::Thread* context, ArrayOffset offset,
                            const void *payload, uint16_t payload_offset, uint16_t payload_count) {
    return pimpl_->overwrite_record(context, offset, payload, payload_offset, payload_count);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
