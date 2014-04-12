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
#include <foedus/storage/array/fwd.hpp>
namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Pimpl object of LogManager.
 * @ingroup LOG
 * @details
 * A private pimpl object for LogManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ArrayStoragePimpl final : public DefaultInitializable {
 public:
    ArrayStoragePimpl() = delete;
    explicit ArrayStoragePimpl(Engine* engine, StorageId storage_id)
        : engine_(engine), storage_id_(storage_id) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Engine* const           engine_;
    StorageId               storage_id_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
