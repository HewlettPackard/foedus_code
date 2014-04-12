/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#define FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/storage/fwd.hpp>
namespace foedus {
namespace storage {
/**
 * @brief Storage Manager class that provides API to create/open/close/drop key-value stores.
 * @ingroup STORAGE
 */
class StorageManager : public virtual Initializable {
 public:
    explicit StorageManager(Engine* engine);
    ~StorageManager();

    // Disable default constructors
    StorageManager() CXX11_FUNC_DELETE;
    StorageManager(const StorageManager&) CXX11_FUNC_DELETE;
    StorageManager& operator=(const StorageManager&) CXX11_FUNC_DELETE;

    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE;
    ErrorStack  uninitialize() CXX11_OVERRIDE;

 private:
    StorageManagerPimpl *pimpl_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
