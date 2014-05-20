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
#include <foedus/thread/fwd.hpp>
#include <string>
namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Represents a key-value store based on a dense and regular array.
 * @ingroup ARRAY
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
     * @param[in] create If true, we newly allocate this array when create() is called.
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
    StorageId           get_id()    const CXX11_OVERRIDE;
    StorageType         get_type()  const CXX11_OVERRIDE { return ARRAY_STORAGE; }
    const std::string&  get_name()  const CXX11_OVERRIDE;
    bool                exists()    const CXX11_OVERRIDE;
    ErrorStack          create(thread::Thread* context) CXX11_OVERRIDE;

    /**
     * @brief Returns byte size of one record in this array storage without internal overheads.
     * @details
     * ArrayStorage is a fix-sized storage, thus we have this interface in storage level
     * rather than in record level.
     */
    uint16_t    get_payload_size() const;

    /** Returns the size of this array. */
    ArrayOffset get_array_size() const;

    /**
     * @brief Retrieves one record of the given offset in this array storage.
     * @param[in] context Thread context
     * @param[in] offset The offset in this array
     * @param[out] payload We copy the record to this address. Must be at least get_payload_size().
     * @pre offset < get_array_size()
     * @details
     * Equivalent to get_record(context, offset, payload, 0, get_payload_size()).
     */
    ErrorStack  get_record(thread::Thread* context, ArrayOffset offset, void *payload) {
        return get_record(context, offset, payload, 0, get_payload_size());
    }
    /**
     * @brief Retrieves a part of record of the given offset in this array storage.
     * @param[in] context Thread context
     * @param[in] offset The offset in this array
     * @param[out] payload We copy the record to this address. Must be at least payload_count.
     * @param[in] payload_offset We copy from this byte position of the record.
     * @param[in] payload_count How many bytes we copy.
     * @pre payload_offset + payload_count <= get_payload_size()
     * @pre offset < get_array_size()
     */
    ErrorStack  get_record(thread::Thread* context, ArrayOffset offset,
                        void *payload, uint16_t payload_offset, uint16_t payload_count);

    /**
     * @brief Retrieves a part of record of the given offset as a primitive type
     * in this array storage. A bit more efficient than get_record().
     * @param[in] context Thread context
     * @param[in] offset The offset in this array
     * @param[out] payload We copy the record to this address.
     * @param[in] payload_offset We copy from this byte position of the record.
     * @tparam T primitive type. All integers and floats are allowed.
     * @pre payload_offset + sizeof(T) <= get_payload_size()
     * @pre offset < get_array_size()
     */
    template <typename T>
    ErrorStack  get_record_primitive(thread::Thread* context, ArrayOffset offset,
                        T *payload, uint16_t payload_offset);

    /**
     * @brief Overwrites one record of the given offset in this array storage.
     * @param[in] context Thread context
     * @param[in] offset The offset in this array
     * @param[in] payload We copy from this buffer. Must be at least get_payload_size().
     * @pre offset < get_array_size()
     * @details
     * Equivalent to overwrite_record(context, offset, payload, 0, get_payload_size()).
     */
    ErrorStack  overwrite_record(thread::Thread* context, ArrayOffset offset, const void *payload) {
        return overwrite_record(context, offset, payload, 0, get_payload_size());
    }
    /**
     * @brief Overwrites a part of one record of the given offset in this array storage.
     * @param[in] context Thread context
     * @param[in] offset The offset in this array
     * @param[in] payload We copy from this buffer. Must be at least get_payload_size().
     * @param[in] payload_offset We copy to this byte position of the record.
     * @param[in] payload_count How many bytes we copy.
     * @pre payload_offset + payload_count <= get_payload_size()
     * @pre offset < get_array_size()
     */
    ErrorStack  overwrite_record(thread::Thread* context, ArrayOffset offset,
                        const void *payload, uint16_t payload_offset, uint16_t payload_count);

    /**
     * @brief Overwrites a part of record of the given offset as a primitive type
     * in this array storage. A bit more efficient than overwrite_record().
     * @param[in] context Thread context
     * @param[in] offset The offset in this array
     * @param[in] payload The value as primitive type.
     * @param[in] payload_offset We copy to this byte position of the record.
     * @tparam T primitive type. All integers and floats are allowed.
     * @pre payload_offset + sizeof(T) <= get_payload_size()
     * @pre offset < get_array_size()
     */
    template <typename T>
    ErrorStack  overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
                        T payload, uint16_t payload_offset);

 private:
    ArrayStoragePimpl*  pimpl_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
