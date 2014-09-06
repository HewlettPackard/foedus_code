/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#define FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#include <string>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/thread/fwd.hpp"

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
   * @brief Issue a unique and atomically/monotonically increasing storage ID for a new storage.
   * @details
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
  Storage*    get_storage(const StorageName& name);

  /**
   * @brief Removes the storage object.
   * @param[in] context thread context to drop the storage
   * @param[in] id ID of the storage to remove
   * @param[out] commit_epoch The epoch at whose end the storage is really deemed as deleted.
   * @details
   * This also invokes uninitialize/destruct.
   * This method is idempotent, although it logs warning for non-existing id.
   */
  ErrorStack  drop_storage(thread::Thread* context, StorageId id, Epoch *commit_epoch);

  /**
   * A convenience function to impersonate as one of available threads and invoke drop_storage().
   * @see drop_storage()
   */
  ErrorStack  drop_storage(StorageId id, Epoch *commit_epoch);

  /**
   * @brief Newly creates a storage with the specified metadata and registers it to this
   * manager.
   * @param[in] context thread context to create this storage
   * @param[in,out] metadata Specifies metadata of the newly created storage, such as name.
   * The metadata object must be an instance of derived metadata such as ArrayMetadata.
   * This method, when succeeded, changes only one property of the given metadata; id_.
   * @param[out] storage Pointer to the created storage, if no error observed.
   * @param[out] commit_epoch The epoch at whose end the storage is really deemed as created.
   */
  ErrorStack  create_storage(thread::Thread* context, Metadata *metadata, Storage **storage,
                             Epoch *commit_epoch);

  /**
   * @brief A convenience function to impersonate as one of available threads
   * and then invoke create_storage().
   */
  ErrorStack  create_storage(Metadata *metadata, Storage **storage, Epoch *commit_epoch);

  /**
   * @brief Just a type-wrapper of create_storage() for array storages.
   * @see create_storage()
   */
  ErrorStack  create_array(
    thread::Thread* context,
    array::ArrayMetadata *metadata,
    array::ArrayStorage **storage,
    Epoch *commit_epoch);

  /**
   * @brief A convenience function to impersonate as one of available threads
   * and then invoke create_array().
   */
  ErrorStack  create_array(
    array::ArrayMetadata *metadata,
    array::ArrayStorage **storage,
    Epoch *commit_epoch);

  /**
   * @brief Just a type-wrapper of create_storage() for hash storages.
   * @see create_storage()
   */
  ErrorStack  create_hash(
    thread::Thread* context,
    hash::HashMetadata *metadata,
    hash::HashStorage **storage,
    Epoch *commit_epoch);

  /**
   * @brief A convenience function to impersonate as one of available threads
   * and then invoke create_hash().
   */
  ErrorStack  create_hash(
    hash::HashMetadata *metadata,
    hash::HashStorage **storage,
    Epoch *commit_epoch);


  /**
   * @brief Just a type-wrapper of create_storage() for sequential storages.
   * @see create_storage()
   */
  ErrorStack  create_sequential(
    thread::Thread* context,
    sequential::SequentialMetadata *metadata,
    sequential::SequentialStorage **storage,
    Epoch *commit_epoch);

  /**
   * @brief A convenience function to impersonate as one of available threads
   * and then invoke create_sequential().
   */
  ErrorStack  create_sequential(
    sequential::SequentialMetadata *metadata,
    sequential::SequentialStorage **storage,
    Epoch *commit_epoch);

  /**
   * @brief Just a type-wrapper of create_storage() for masstree storages.
   * @see create_storage()
   */
  ErrorStack  create_masstree(
    thread::Thread* context,
    masstree::MasstreeMetadata *metadata,
    masstree::MasstreeStorage **storage,
    Epoch *commit_epoch);

  /**
   * @brief A convenience function to impersonate as one of available threads
   * and then invoke create_masstree().
   */
  ErrorStack  create_masstree(
    masstree::MasstreeMetadata *metadata,
    masstree::MasstreeStorage **storage,
    Epoch *commit_epoch);

  /**
   * This method is called during snapshotting to clone metadata of all existing storages
   * to the given object. So far, this method is not quite optimized for the case where
   * there are many thousands of storages. There are many other things to do before that,
   * but at some point we must do something.
   */
  ErrorStack  clone_all_storage_metadata(snapshot::SnapshotMetadata *metadata);

  /** Returns pimpl object. Use this only if you know what you are doing. */
  StorageManagerPimpl* get_pimpl() { return pimpl_; }

 private:
  StorageManagerPimpl *pimpl_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
