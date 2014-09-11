/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
/**
 * @brief Pimpl object of StorageManager.
 * @ingroup STORAGE
 * @details
 * A private pimpl object for StorageManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class StorageManagerPimpl final : public DefaultInitializable {
 public:
  /** Calculates required byte size of shared memory for this module. */
  static uint64_t get_required_shared_memory_size(const EngineOptions& options);

  StorageManagerPimpl() = delete;
  explicit StorageManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  void        init_storage_factories();
  void        clear_storage_factories();

  StorageId   issue_next_storage_id();
  Storage*    get_storage(StorageId id);
  Storage*    get_storage(const StorageName& name);
  /**
   * @brief Adds a storage object, either newly created or constructed from disk at start-up.
   * @param[in] storage an already-constructred and initialized Storage
   * @details
   * The ownership is handed over to this manager, thus caller should NOT uninitialize/destruct.
   */
  ErrorStack  register_storage(Storage* storage);
  ErrorStack  expand_storage_array(StorageId new_size);

  ErrorStack  drop_storage(thread::Thread* context, StorageId id, Epoch *commit_epoch);
  ErrorStack  drop_storage(StorageId id, Epoch *commit_epoch);
  void        drop_storage_apply(thread::Thread* context, Storage* storage);

  ErrorStack  create_storage(thread::Thread*, Metadata *metadata, Storage **storage,
                             Epoch *commit_epoch);
  ErrorStack  create_storage(Metadata *metadata, Storage **storage, Epoch *commit_epoch);

  ErrorStack  clone_all_storage_metadata(snapshot::SnapshotMetadata *metadata);

  void*       get_instance_memory(StorageId storage_id) {
    return reinterpret_cast<char*>(instance_memory_.get_block())
      + static_cast<uint64_t>(storage_id) * kPageSize;
  }

  Engine* const           engine_;

  /**
   * In case there are multiple threads that add/delete/expand storages,
   * those threads take this lock.
   * Normal threads that only read storages_ don't have to take this.
   */
  std::mutex              mod_lock_;

  /**
   * The largest StorageId we so far observed.
   * This value +1 would be the ID of the storage created next.
   */
  StorageId               largest_storage_id_;

  /** Same as engine_->get_options().storage_.max_storages_ */
  StorageId               max_storages_;

  /**
   * Pointers of all Storage objects in this engine.
   * If there is a hole, it contains a nullptr.
   */
  Storage**               storages_;
  /**
   * Capacity of storages_. When we need an expansion, we do RCU and switches the pointer.
   */
  size_t                  storages_capacity_;

  /**
   * Storage instances (pimpl objects) are allocated in this shared memory.
   * Each storage instance must be within 4kb. If the storage type requires more than 4kb,
   * just grab a page from volatile page pools and point to it from the storage object.
   * Remember that all pages in volatile page pools are shared.
   */
  memory::AlignedMemory   instance_memory_;

  /**
   * Storage name to pointer mapping. Accessing this, either read or write, must take mod_lock_
   * because std::map is not thread-safe. This is why get_storage(string) is more expensive.
   */
  std::map< StorageName, Storage* >   storage_names_;

  /**
   * Factory objects to instantiate storage objects.
   * This is just a vector, so you must iterate over it and invoke is_right_metadata() to find
   * the right factory for the given metadata.
   */
  std::vector< StorageFactory* >      storage_factories_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_MANAGER_PIMPL_HPP_
