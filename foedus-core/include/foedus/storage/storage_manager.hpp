/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#define FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

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

  Engine*     get_engine() const;
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /**
   * Special method called only from recovery manager.
   */
  ErrorStack  reinitialize_for_recovered_snapshot();

  /**
   * @brief Issue a unique and atomically/monotonically increasing storage ID for a new storage.
   * @details
   * The caller might later fail, so StorageId might have holes.
   */
  StorageId   issue_next_storage_id();
  /** Returns the largest StorageId that does or did exist. */
  StorageId   get_largest_storage_id();

  /**
   * @brief Returns the name of the given storage ID.
   * @return name of the storage. If the ID doesn't exist, an empty string.
   */
  const StorageName& get_name(StorageId id);

  /**
   * Returns the storage of given ID.
   * @param[in] id Storage ID
   * @return Control block of the storage in this engine. If there is no storage with the ID, the
   * returned control block is not initialized (though not null).
   */
  StorageControlBlock* get_storage(StorageId id);

  /**
   * Returns the array storage of given ID.
   * @param[in] id Storage ID
   * @return Array Storage. If there is no storage with the ID, an empty object.
   */
  array::ArrayStorage get_array(StorageId id) {
    return array::ArrayStorage(get_engine(), get_storage(id));
  }

  /**
   * Returns the hash storage of given ID.
   * @param[in] id Storage ID
   * @return Hash Storage. If there is no storage with the ID, an empty object.
   */
  hash::HashStorage get_hash(StorageId id) {
    return hash::HashStorage(get_engine(), get_storage(id));
  }

  /**
   * Returns the sequential storage of given ID.
   * @param[in] id Storage ID
   * @return Sequential Storage. If there is no storage with the ID, an empty object.
   */
  sequential::SequentialStorage get_sequential(StorageId id) {
    return sequential::SequentialStorage(get_engine(), get_storage(id));
  }

  /**
   * Returns the masstree storage of given ID.
   * @param[in] id Storage ID
   * @return Masstree Storage. If there is no storage with the ID, an empty object.
   */
  masstree::MasstreeStorage get_masstree(StorageId id) {
    return masstree::MasstreeStorage(get_engine(), get_storage(id));
  }

  /**
   * Returns the storage of given name.
   * This one is convenient, but prefer get_storage(StorageId) for better performance.
   * Or, write your code so that you don't have to invoke this method too often.
   * @param[in] name Storage name
   * @return Control block of the storage in this engine. If there is no storage with the name,
   * the returned control block is not initialized (though not null).
   */
  StorageControlBlock* get_storage(const StorageName& name);

  /**
   * Returns the array storage of given name.
   * @param[in] name Storage name
   * @return Array Storage. If there is no storage with the name, an empty object.
   */
  array::ArrayStorage get_array(const StorageName& name) {
    return array::ArrayStorage(get_engine(), get_storage(name));
  }

  /**
   * Returns the hash storage of given name.
   * @param[in] name Storage name
   * @return Hash Storage. If there is no storage with the name, an empty object.
   */
  hash::HashStorage get_hash(const StorageName& name) {
    return hash::HashStorage(get_engine(), get_storage(name));
  }

  /**
   * Returns the sequential storage of given name.
   * @param[in] name Storage name
   * @return Sequential Storage. If there is no storage with the name, an empty object.
   */
  sequential::SequentialStorage get_sequential(const StorageName& name) {
    return sequential::SequentialStorage(get_engine(), get_storage(name));
  }

  /**
   * Returns the masstree storage of given name.
   * @param[in] name Storage name
   * @return Masstree Storage. If there is no storage with the name, an empty object.
   */
  masstree::MasstreeStorage get_masstree(const StorageName& name) {
    return masstree::MasstreeStorage(get_engine(), get_storage(name));
  }

  /**
   * @brief Removes the storage object.
   * @param[in] id ID of the storage to remove
   * @param[out] commit_epoch The epoch at whose end the storage is really deemed as deleted.
   * @details
   * This method is idempotent, although it logs warning for non-existing id.
   */
  ErrorStack  drop_storage(StorageId id, Epoch *commit_epoch);
  /** This is called while restart to apply DROP STORAGE logs. */
  void        drop_storage_apply(StorageId id);

  /**
   * @brief Newly creates a storage with the specified metadata and registers it to this
   * manager.
   * @param[in,out] metadata Specifies metadata of the newly created storage, such as name.
   * The metadata object must be an instance of derived metadata such as ArrayMetadata.
   * This method, when succeeded, changes only one property of the given metadata; id_.
   * @param[out] commit_epoch The epoch at whose end the storage is really deemed as created.
   */
  ErrorStack  create_storage(Metadata *metadata, Epoch *commit_epoch);
  /** This is called while restart to apply CREATE STORAGE logs. */
  void        create_storage_apply(const Metadata& metadata);

  /**
   * @brief Just a type-wrapper of create_storage() for array storages.
   * @see create_storage()
   */
  ErrorStack  create_array(
    array::ArrayMetadata *metadata,
    array::ArrayStorage *storage,
    Epoch *commit_epoch);

  /**
   * @brief Just a type-wrapper of create_storage() for hash storages.
   * @see create_storage()
   */
  ErrorStack  create_hash(
    hash::HashMetadata *metadata,
    hash::HashStorage *storage,
    Epoch *commit_epoch);

  /**
   * @brief Just a type-wrapper of create_storage() for sequential storages.
   * @see create_storage()
   */
  ErrorStack  create_sequential(
    sequential::SequentialMetadata *metadata,
    sequential::SequentialStorage *storage,
    Epoch *commit_epoch);

  /**
   * @brief Just a type-wrapper of create_storage() for masstree storages.
   * @see create_storage()
   */
  ErrorStack  create_masstree(
    masstree::MasstreeMetadata *metadata,
    masstree::MasstreeStorage *storage,
    Epoch *commit_epoch);

  /**
   * This method is called during snapshotting to clone metadata of all existing storages
   * to the given object.
   */
  ErrorStack  clone_all_storage_metadata(snapshot::SnapshotMetadata *metadata);

  /**
   * @brief Resolves a "moved" record.
   * @details
   * This is the core of the moved-bit protocol. Receiving a xct_id address that points
   * to a moved record, track the physical record in another page.
   * This method does not take lock, so it is possible that concurrent threads
   * again move the record after this.
   * The only case it fails to track is the record moved to deeper layers. If the write-set
   * is supplied, we use the key information in it to track even in that case.
   */
  xct::TrackMovedRecordResult track_moved_record(
    StorageId storage_id,
    xct::RwLockableXctId* old_address,
    xct::WriteXctAccess* write_set);

  /** Returns pimpl object. Use this only if you know what you are doing. */
  StorageManagerPimpl* get_pimpl() { return pimpl_; }

 private:
  StorageManagerPimpl *pimpl_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_HPP_
