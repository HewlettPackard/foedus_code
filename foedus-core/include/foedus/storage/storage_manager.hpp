/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#define FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/storage/fwd.hpp>
#include <foedus/storage/storage_id.hpp>
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

    /**
     * Issue a unique and atomically/monotonically increasing storage ID for a new storage.
     * The caller might later fail, so StorageId might have holes.
     */
    StorageId   issue_next_storage_id();

    /**
     * Returns the storage of given ID.
     * @param[in] id Storage ID
     * @return Storage object in this engine. If there is no storage with the ID, nullptr.
     */
    Storage*    get_storage(StorageId id);

    /**
     * @brief Adds a storage object, either newly created or constructed from disk at start-up.
     * @param[in] storage an already-constructred and initialized Storage
     * @details
     * The ownership is handed over to this manager, thus caller should NOT uninitialize/destruct.
     */
    ErrorStack  register_storage(Storage* storage);

    /**
     * Removes the storage object.
     * This also invokes uninitialize/destruct.
     * This method is idempotent, although it logs warning for non-existing id.
     */
    ErrorStack  remove_storage(StorageId id);

 private:
    StorageManagerPimpl *pimpl_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
