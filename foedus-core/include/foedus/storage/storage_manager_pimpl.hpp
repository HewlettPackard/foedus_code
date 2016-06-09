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
#ifndef FOEDUS_STORAGE_STORAGE_MANAGER_PIMPL_HPP_
#define FOEDUS_STORAGE_STORAGE_MANAGER_PIMPL_HPP_
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
/** Shared data in StorageManagerPimpl. */
struct StorageManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  StorageManagerControlBlock() = delete;
  ~StorageManagerControlBlock() = delete;

  void initialize() {
    mod_lock_.initialize();
  }
  void uninitialize() {
    mod_lock_.uninitialize();
  }

  /**
   * In case there are multiple threads that add/delete/expand storages,
   * those threads take this lock.
   * Normal threads that only read storages_ don't have to take this.
   */
  soc::SharedMutex        mod_lock_;

  /**
   * The largest StorageId we so far observed.
   * This value +1 would be the ID of the storage created next.
   */
  StorageId               largest_storage_id_;
};

/**
 * @brief Pimpl object of StorageManager.
 * @ingroup STORAGE
 * @details
 * A private pimpl object for StorageManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class StorageManagerPimpl final : public DefaultInitializable {
 public:
  StorageManagerPimpl() = delete;
  explicit StorageManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  initialize_read_latest_snapshot();
  ErrorStack  uninitialize_once() override;

  /**
   * Special method called only from recovery manager.
   * 1) initialize_once(), which calls initialize_read_latest_snapshot().
   * 2) if there are non-snapshotted and durable logs, recovery manager triggers snapshotting
   * 3) then, reinitialize only root pages with this method.
   *
   * Unlike usual snapshot, we can assume that there is no concurrent worker,
   * so we can safely drop volatile root pages and re-create them with the new snapshot pages.
   */
  ErrorStack  reinitialize_for_recovered_snapshot();

  StorageId   issue_next_storage_id();
  StorageControlBlock*  get_storage(StorageId id) { return &storages_[id]; }
  StorageControlBlock*  get_storage(const StorageName& name);
  bool                  exists(const StorageName& name);

  ErrorStack  drop_storage(StorageId id, Epoch *commit_epoch);
  void        drop_storage_apply(StorageId id);
  ErrorStack  create_storage(Metadata *metadata, Epoch *commit_epoch);
  void        create_storage_apply(const Metadata& metadata);
  template <typename STORAGE>
  ErrorStack  create_storage_and_log(const Metadata* meta, Epoch *commit_epoch);

  /**
   * Resets all volatile pages' temperature stat to be zero in the specified storage.
   * Used only in HCC-branch.
   */
  ErrorStack  hcc_reset_all_temperature_stat(StorageId storage_id);

  xct::TrackMovedRecordResult track_moved_record(
    StorageId storage_id,
    xct::RwLockableXctId* old_address,
    xct::WriteXctAccess *write);
  ErrorStack  clone_all_storage_metadata(snapshot::SnapshotMetadata *metadata);

  uint32_t    get_max_storages() const;

  Engine* const           engine_;

  StorageManagerControlBlock* control_block_;

  /**
   * Storage instances (pimpl objects) are allocated in this shared memory.
   * Each storage instance must be within 4kb. If the storage type requires more than 4kb,
   * just grab a page from volatile page pools and point to it from the storage object.
   * Remember that all pages in volatile page pools are shared.
   */
  StorageControlBlock*    storages_;

  /**
   * This shared memory stores the ID of storages sorted by their names.
   * Accessing this, either read or write, must take mod_lock_.
   * This is why get_storage(string) is more expensive.
   */
  storage::StorageId*     storage_name_sort_;
};

static_assert(
  sizeof(StorageManagerControlBlock) <= soc::GlobalMemoryAnchors::kStorageManagerMemorySize,
  "StorageManagerControlBlock is too large.");
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_PIMPL_HPP_
