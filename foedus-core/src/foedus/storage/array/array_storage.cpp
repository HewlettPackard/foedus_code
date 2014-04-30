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

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace array
}  // namespace storage
}  // namespace foedus
