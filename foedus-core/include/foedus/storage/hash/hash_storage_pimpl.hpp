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
  /**
   * How many hash bins this storage has.
   * bin_count_ = 2^bin_bits
   */
  uint64_t            bin_count_;
  /**
   * Number of levels of pages. 1 means there is only 1 intermediate page pointing to data pages.
   * 2 means a root page pointing down to leaf intermediate pages pointing to data pages.
   * At least 1, and surely within 8 levels.
   */
  uint8_t             levels_;
  char                padding_[7];
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
    HashIntermediatePage* page,
    VolatilePagePointer volatile_page_id);
  void        release_pages_recursive_intermediate(
    memory::PageReleaseBatch* batch,
    HashIntermediatePage* page,
    VolatilePagePointer volatile_page_id);
  void        release_pages_recursive_data(
    memory::PageReleaseBatch* batch,
    HashDataPage* page,
    VolatilePagePointer volatile_page_id);


  ErrorStack  create(const HashMetadata& metadata);
  ErrorStack  load(const StorageControlBlock& snapshot_block);
  ErrorStack  drop();
  HashIntermediatePage* get_root_page();

  bool                exists()    const { return control_block_->exists(); }
  StorageId           get_id()    const { return control_block_->meta_.id_; }
  const StorageName&  get_name()  const { return control_block_->meta_.name_; }
  const HashMetadata& get_meta()  const { return control_block_->meta_; }
  uint8_t             get_levels() const { return control_block_->levels_; }
  HashBin             get_bin_count() const { return get_meta().get_bin_count(); }
  uint8_t             get_bin_bits() const { return get_meta().bin_bits_; }
  uint8_t             get_bin_shifts() const { return get_meta().get_bin_shifts(); }

  /** @copydoc foedus::storage::hash::HashStorage::get_record() */
  ErrorCode   get_record(
    thread::Thread* context,
    const HashCombo& combo,
    void* payload,
    uint16_t* payload_capacity);

  /** @copydoc foedus::storage::hash::HashStorage::get_record_primitive() */
  template <typename PAYLOAD>
  ErrorCode   get_record_primitive(
    thread::Thread* context,
    const HashCombo& combo,
    PAYLOAD* payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::hash::HashStorage::get_record_part() */
  ErrorCode   get_record_part(
    thread::Thread* context,
    const HashCombo& combo,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @copydoc foedus::storage::hash::HashStorage::insert_record() */
  ErrorCode insert_record(
    thread::Thread* context,
    const HashCombo& combo,
    const void* payload,
    uint16_t payload_count);

  /** @copydoc foedus::storage::hash::HashStorage::delete_record() */
  ErrorCode delete_record(thread::Thread* context, const HashCombo& combo);

  /** @copydoc foedus::storage::hash::HashStorage::overwrite_record() */
  ErrorCode overwrite_record(
    thread::Thread* context,
    const HashCombo& combo,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @copydoc foedus::storage::hash::HashStorage::overwrite_record_primitive() */
  template <typename PAYLOAD>
  ErrorCode   overwrite_record_primitive(
    thread::Thread* context,
    const HashCombo& combo,
    PAYLOAD payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::hash::HashStorage::increment_record() */
  template <typename PAYLOAD>
  ErrorCode   increment_record(
    thread::Thread* context,
    const HashCombo& combo,
    PAYLOAD* value,
    uint16_t payload_offset);

  /** Result of lookup_bin() */
  struct BinSearchResult {
    /** Address of a pointer to the head data page that corresponds to the searched bin. */
    DualPagePointer* pointer_address_;
  };

  /**
   * @brief Find a pointer to the bin that contains records for the hash.
   * @param[in] context Thread context
   * @param[in] for_write Whether we are reading these pages to modify
   * @param[in] combo Hash values. Also the result of this method.
   * @param[out] result Pointer to a data page.
   * @details
   * If the search is for-write search, we always create a volatile page, even recursively,
   * thus pointer_address_!= null and also pointer_address_->volatile_pointer_!=null.
   *
   * If the search is a for-read search and also the corresponding data page or its ascendants
   * do not exist yet, pointer_address_ is still non null, but it might be an address to
   * a DualPagePointer in higher level, and also pointer_address_->volatile_pointer_
   * or pointer_address_->snapshot_pointer_ might be null.
   * In that case, you can just return "not found" as a result.
   * lookup_bin() internally adds a pointer set to protect the result, too.
   */
  ErrorCode   lookup_bin(
    thread::Thread* context,
    bool for_write,
    const HashCombo& combo,
    BinSearchResult* result);

  ErrorCode   locate_record(
    thread::Thread* context,
    bool for_write,
    const HashCombo& combo) ALWAYS_INLINE;
};
static_assert(sizeof(HashStoragePimpl) <= kPageSize, "HashStoragePimpl is too large");
static_assert(
  sizeof(HashStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "HashStorageControlBlock is too large.");
}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_
