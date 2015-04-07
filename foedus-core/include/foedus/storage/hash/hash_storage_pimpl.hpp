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
#ifndef FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/attachable.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace storage {
namespace hash {
/** Shared data of this storage type */
struct HashStorageControlBlock final {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  HashStorageControlBlock() = delete;
  ~HashStorageControlBlock() = delete;

  bool exists() const { return status_ == kExists || status_ == kMarkedForDeath; }

  soc::SharedMutex    status_mutex_;
  /** Status of the storage */
  StorageStatus       status_;
  /** Points to the root page (or something equivalent). */
  DualPagePointer     root_page_pointer_;
  /** metadata of this storage. */
  HashMetadata        meta_;

  // Do NOT reorder members up to here. The layout must be compatible with StorageControlBlock
  // Type-specific shared members below.

  // these are static auxiliary variables. doesn't have to be shared actually, but easier.
  uint32_t            root_pages_;
  uint64_t            bin_count_;
  uint64_t            bin_pages_;
};

/**
* @brief Pimpl object of HashStorage.
 * @ingroup HASH
 * @details
 * A private pimpl object for HashStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class HashStoragePimpl final : public Attachable<HashStorageControlBlock> {
 public:
  HashStoragePimpl() : Attachable<HashStorageControlBlock>() {}
  explicit HashStoragePimpl(HashStorage* storage)
    : Attachable<HashStorageControlBlock>(
      storage->get_engine(),
      storage->get_control_block()) {}

  /** Used only from uninitialize() */
  void        release_pages_recursive_root(
    memory::PageReleaseBatch* batch,
    HashRootPage* page,
    VolatilePagePointer volatile_page_id);
  void        release_pages_recursive_bin(
    memory::PageReleaseBatch* batch,
    HashBinPage* page,
    VolatilePagePointer volatile_page_id);
  void        release_pages_recursive_data(
    memory::PageReleaseBatch* batch,
    HashDataPage* page,
    VolatilePagePointer volatile_page_id);


  ErrorStack  create(const HashMetadata& metadata);
  ErrorStack  load(const StorageControlBlock& snapshot_block);
  ErrorStack  drop();
  HashRootPage* get_root_page();

  bool                exists()    const { return control_block_->exists(); }
  StorageId           get_id()    const { return control_block_->meta_.id_; }
  const StorageName&  get_name()  const { return control_block_->meta_.name_; }
  const HashMetadata& get_meta()  const { return control_block_->meta_; }
  uint8_t             get_bin_bits() const { return get_meta().bin_bits_; }
  uint32_t            get_root_pages() const { return control_block_->root_pages_; }
  uint64_t            get_bin_count() const { return control_block_->bin_count_; }
  uint64_t            get_bin_pages() const { return control_block_->bin_pages_; }

  /** @copydoc foedus::storage::hash::HashStorage::get_record() */
  ErrorCode   get_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    void* payload,
    uint16_t* payload_capacity);

  /** @copydoc foedus::storage::hash::HashStorage::get_record_primitive() */
  template <typename PAYLOAD>
  ErrorCode   get_record_primitive(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    PAYLOAD* payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::hash::HashStorage::get_record_part() */
  ErrorCode   get_record_part(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @copydoc foedus::storage::hash::HashStorage::insert_record() */
  ErrorCode insert_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_count);

  /** @copydoc foedus::storage::hash::HashStorage::delete_record() */
  ErrorCode delete_record(thread::Thread* context, const void* key, uint16_t key_length);

  /** @copydoc foedus::storage::hash::HashStorage::overwrite_record() */
  ErrorCode overwrite_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @copydoc foedus::storage::hash::HashStorage::overwrite_record_primitive() */
  template <typename PAYLOAD>
  ErrorCode   overwrite_record_primitive(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    PAYLOAD payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::hash::HashStorage::increment_record() */
  template <typename PAYLOAD>
  ErrorCode   increment_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    PAYLOAD* value,
    uint16_t payload_offset);

  void      apply_insert_record(
    thread::Thread* context,
    const HashInsertLogType* log_entry,
    xct::LockableXctId* owner_id,
    char* payload);
  void      apply_delete_record(
    thread::Thread* context,
    const HashDeleteLogType* log_entry,
    xct::LockableXctId* owner_id,
    char* payload);

  /**
   * @brief Find a bin page that contains a bin for the hash.
   * @param[in] context Thread context
   * @param[in] for_write Whether we are reading these pages to modify
   * @param[in,out] combo Hash values. Also the result of this method.
   * @details
   * It might set null to out if there is no bin page created yet.
   * In this case, the pointer to the bin page is added as a node set to capture a concurrent
   * event installing a new volatile page there.
   */
  ErrorCode     lookup_bin(thread::Thread* context, bool for_write, HashCombo *combo);

  HashRootPage* lookup_boundary_root(
    thread::Thread* context,
    uint64_t bin,
    uint16_t *pointer_index) ALWAYS_INLINE;

  ErrorCode     locate_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    HashCombo* combo) ALWAYS_INLINE;
};
static_assert(sizeof(HashStoragePimpl) <= kPageSize, "HashStoragePimpl is too large");
static_assert(
  sizeof(HashStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "HashStorageControlBlock is too large.");
}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_
