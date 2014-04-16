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
#include <foedus/storage/array/array_id.hpp>
#include <foedus/storage/array/fwd.hpp>
#include <string>
namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Represents a key-value store based on a dense and regular array.
 * @ingroup STORAGE
 */
class ArrayStorage CXX11_FINAL : public virtual Storage {
 public:
    /**
     * Constructs an array storage either from disk or newly create.
     * @param[in] engine Database engine
     * @param[in] id Unique ID of this storage
     * @param[in] name Name of this storage
     * @param[in] payload_size byte size of one record in this array storage
     * without internal overheads.
     * @param[in] array_size Size of this array
     * @param[in] root_page Root page of this array, ignored if create=true
     * @param[in] create If true, we newly allocate this array when initialize() is called.
     */
    ArrayStorage(Engine* engine, StorageId id, const std::string &name, uint16_t payload_size,
        ArrayOffset array_size, DualPagePointer root_page, bool create);
    ~ArrayStorage() CXX11_OVERRIDE;

    // Disable default constructors
    ArrayStorage() CXX11_FUNC_DELETE;
    ArrayStorage(const ArrayStorage&) CXX11_FUNC_DELETE;
    ArrayStorage& operator=(const ArrayStorage&) CXX11_FUNC_DELETE;

    // Initializable interface
    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE;
    ErrorStack  uninitialize() CXX11_OVERRIDE;

    // Storage interface
    StorageId           get_id() const CXX11_OVERRIDE;
    const std::string&  get_name() const CXX11_OVERRIDE;

    /** Returns byte size of one record in this array storage without internal overheads. */
    uint16_t    get_payload_size() const;
    /** Size of this array. */
    ArrayOffset get_array_size() const;

    ErrorStack  get_record(ArrayOffset offset, void *payload);
    ErrorStack  get_record_part(ArrayOffset offset, void *payload,
                                uint16_t payload_offset, uint16_t payload_count);

    ErrorStack  overwrite_record(ArrayOffset offset, const void *payload);
    ErrorStack  overwrite_record_part(ArrayOffset offset, const void *payload,
                                uint16_t payload_offset, uint16_t payload_count);

 private:
    ArrayStoragePimpl*  pimpl_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
