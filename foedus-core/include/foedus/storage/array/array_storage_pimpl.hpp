/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
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
 * @brief Pimpl object of ArrayStorage.
 * @ingroup LOG
 * @details
 * A private pimpl object for ArrayStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ArrayStoragePimpl final : public DefaultInitializable {
 public:
    ArrayStoragePimpl() = delete;
    ArrayStoragePimpl(Engine* engine, StorageId id, const std::string &name, uint16_t payload_size,
        ArrayOffset array_size, DualPagePointer root_page, bool create);

    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    ErrorStack  create();

    ErrorStack  get_record(ArrayOffset offset, void *payload);
    ErrorStack  get_record_part(ArrayOffset offset, void *payload,
                                uint16_t payload_offset, uint16_t payload_count);

    ErrorStack  overwrite_record(ArrayOffset offset, const void *payload);
    ErrorStack  overwrite_record_part(ArrayOffset offset, const void *payload,
                                uint16_t payload_offset, uint16_t payload_count);

    Engine* const           engine_;
    const StorageId         id_;
    const std::string       name_;
    const uint16_t          payload_size_;
    const uint16_t          payload_size_aligned_;
    const ArrayOffset       array_size_;
    DualPagePointer         root_page_;
    bool                    create_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
