/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/storage/storage.hpp>
#include <foedus/storage/storage_id.hpp>
#include <foedus/storage/array/fwd.hpp>
namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Represents a key-value store based on a dense and regular array.
 * @ingroup STORAGE
 */
class ArrayStorage CXX11_FINAL : public virtual Storage {
 public:
    explicit ArrayStorage(Engine* engine, StorageId storage_id);
    ~ArrayStorage() CXX11_OVERRIDE;

    // Disable default constructors
    ArrayStorage() CXX11_FUNC_DELETE;
    ArrayStorage(const ArrayStorage&) CXX11_FUNC_DELETE;
    ArrayStorage& operator=(const ArrayStorage&) CXX11_FUNC_DELETE;

    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE;
    ErrorStack  uninitialize() CXX11_OVERRIDE;

    StorageId   get_storage_id() const CXX11_OVERRIDE;

 private:
    ArrayStoragePimpl*  pimpl_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
