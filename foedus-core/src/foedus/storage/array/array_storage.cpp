/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_storage_pimpl.hpp>
#include <iostream>
#include <string>
namespace foedus {
namespace storage {
namespace array {
ArrayStorage::ArrayStorage(Engine* engine, const ArrayMetadata &metadata, bool create)
    : pimpl_(nullptr) {
    pimpl_ = new ArrayStoragePimpl(engine, this, metadata, create);
}
ArrayStorage::~ArrayStorage() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack  ArrayStorage::initialize()              { return pimpl_->initialize(); }
ErrorStack  ArrayStorage::uninitialize()            { return pimpl_->uninitialize(); }
ErrorStack  ArrayStorage::create(thread::Thread* context)   { return pimpl_->create(context); }

void ArrayStorage::describe(std::ostream* o_ptr) const {
    std::ostream& o = *o_ptr;
    o << "<ArrayStorage>"
        << "<id>" << get_id() << "</id>"
        << "<name>" << get_name() << "</name>"
        << "<payload_size>" << get_payload_size() << "</payload_size>"
        << "<array_size>" << get_array_size() << "</array_size>"
        << "</ArrayStorage>";
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace array
}  // namespace storage
}  // namespace foedus
