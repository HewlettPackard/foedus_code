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
#include <foedus/storage/array/fwd.hpp>
#include <foedus/storage/array/array_id.hpp>
#include <foedus/thread/fwd.hpp>
#include <string>
namespace foedus {
namespace storage {
/**
 * @brief Storage Manager class that provides API to create/open/close/drop key-value stores.
 * @ingroup STORAGE
 */
class StorageManager CXX11_FINAL : public virtual Initializable {
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
     * Returns the storage of given name.
     * This one is convenient, but prefer get_storage(StorageId) for better performance.
     * Or, write your code so that you don't have to invoke this method too often.
     * @param[in] name Storage name
     * @return Storage object in this engine. If there is no storage with the name, nullptr.
     */
    Storage*    get_storage(const std::string &name);

    /**
     * @brief Adds a storage object, either newly created or constructed from disk at start-up.
     * @param[in] storage an already-constructred and initialized Storage
     * @details
     * The ownership is handed over to this manager, thus caller should NOT uninitialize/destruct.
     */
    ErrorStack  register_storage(Storage* storage);

    /**
     * @brief Removes the storage object.
     * @param[in] id ID of the storage to remove
     * @details
     * This also invokes uninitialize/destruct.
     * This method is idempotent, although it logs warning for non-existing id.
     */
    ErrorStack  remove_storage(StorageId id);

    /**
     * @brief Newly creates an \ref ARRAY with the specified parameters and registers it to this
     * manager.
     * @param[in] context thread context to create this array
     * @param[in] name Name of the array storage
     * @param[in] payload_size byte size of one record in this array storage
     * without internal overheads.
     * @param[in] array_size Size of this array
     * @param[out] out Pointer to the created array storage, if no error observed.
     * @details
     *
     */
    ErrorStack  create_array(thread::Thread* context, const std::string &name,
                uint16_t payload_size, array::ArrayOffset array_size, array::ArrayStorage **out);

    /**
     * A convenience function to impersonate as one of available threads and invoke create_array().
     * @see create_array()
     */
    ErrorStack  create_array_impersonate(const std::string &name,
                uint16_t payload_size, array::ArrayOffset array_size, array::ArrayStorage **out);

 private:
    StorageManagerPimpl *pimpl_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
