/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_MANAGER_PIMPL_HPP_
#define FOEDUS_STORAGE_STORAGE_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/storage/fwd.hpp>
namespace foedus {
namespace storage {
/**
 * @brief Pimpl object of StorageManager.
 * @ingroup STORAGE
 * @details
 * A private pimpl object for StorageManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class StorageManagerPimpl : public DefaultInitializable {
 public:
    StorageManagerPimpl() = delete;
    explicit StorageManagerPimpl(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Engine* const           engine_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_PIMPL_HPP_
