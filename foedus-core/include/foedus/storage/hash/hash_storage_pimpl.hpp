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
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
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
  /** @return the number of child pointers in the root page for this storage */
  uint16_t get_root_children() const {
    return assorted::int_div_ceil(bin_count_, kHashMaxBins[levels_ - 1U]);
  }

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


  xct::TrackMovedRecordResult track_moved_record(
    xct::LockableXctId* old_address,
    xct::WriteXctAccess* write_set);
  xct::TrackMovedRecordResult track_moved_record_search(
    HashDataPage* page,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo);

  /** These are defined in hash_storage_verify.cpp */
  ErrorStack  verify_single_thread(Engine* engine);
  ErrorStack  verify_single_thread(thread::Thread* context);
  ErrorStack  verify_single_thread_intermediate(Engine* engine, HashIntermediatePage* page);
  ErrorStack  verify_single_thread_data(Engine* engine, HashDataPage* head);

  /** These are defined in hash_storage_debug.cpp */
  ErrorStack debugout_single_thread(
    Engine* engine,
    bool volatile_only,
    bool intermediate_only,
    uint32_t max_pages);
  ErrorStack debugout_single_thread_intermediate(
    Engine* engine,
    cache::SnapshotFileSet* fileset,
    HashIntermediatePage* parent,
    bool follow_volatile,
    bool intermediate_only,
    uint32_t* remaining_pages);
  ErrorStack debugout_single_thread_data(
    Engine* engine,
    cache::SnapshotFileSet* fileset,
    HashDataPage* head,
    bool follow_volatile,
    uint32_t* remaining_pages);

  ErrorStack  create(const HashMetadata& metadata);
  ErrorStack  load(const StorageControlBlock& snapshot_block);
  ErrorStack  drop();

  bool                exists()    const { return control_block_->exists(); }
  StorageId           get_id()    const { return control_block_->meta_.id_; }
  const StorageName&  get_name()  const { return control_block_->meta_.name_; }
  const HashMetadata& get_meta()  const { return control_block_->meta_; }
  uint8_t             get_levels() const { return control_block_->levels_; }
  HashBin             get_bin_count() const { return get_meta().get_bin_count(); }
  uint8_t             get_bin_bits() const { return get_meta().bin_bits_; }
  uint8_t             get_bin_shifts() const { return get_meta().get_bin_shifts(); }

  /** @see foedus::storage::hash::HashStorage::get_record() */
  ErrorCode   get_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    void* payload,
    uint16_t* payload_capacity);

  /** @see foedus::storage::hash::HashStorage::get_record_primitive() */
  template <typename PAYLOAD>
  inline ErrorCode get_record_primitive(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    PAYLOAD* payload,
    uint16_t payload_offset) {
    // at this point, there isn't enough benefit to do optimization specific to this case.
    // hash-lookup is anyway dominant. memcpy-vs-primitive is not the issue.
    return get_record_part(
      context,
      key,
      key_length,
      combo,
      payload,
      payload_offset,
      sizeof(PAYLOAD));
  }

  /** @see foedus::storage::hash::HashStorage::get_record_part() */
  ErrorCode   get_record_part(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @see foedus::storage::hash::HashStorage::insert_record() */
  ErrorCode insert_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    const void* payload,
    uint16_t payload_count);

  /** @see foedus::storage::hash::HashStorage::delete_record() */
  ErrorCode delete_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo);

  /** @see foedus::storage::hash::HashStorage::overwrite_record() */
  ErrorCode overwrite_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @see foedus::storage::hash::HashStorage::overwrite_record_primitive() */
  template <typename PAYLOAD>
  inline ErrorCode overwrite_record_primitive(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    PAYLOAD payload,
    uint16_t payload_offset) {
    // same above. still handy as an API, though.
    return overwrite_record(
      context,
      key,
      key_length,
      combo,
      &payload,
      payload_offset,
      sizeof(payload));
  }

  /** @see foedus::storage::hash::HashStorage::increment_record() */
  template <typename PAYLOAD>
  ErrorCode   increment_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    PAYLOAD* value,
    uint16_t payload_offset);

  /**
   * Retrieves the root page of this storage.
   */
  ErrorCode   get_root_page(
    thread::Thread* context,
    bool for_write,
    HashIntermediatePage** root);
  /** for non-root */
  ErrorCode   follow_page(
    thread::Thread* context,
    bool for_write,
    HashIntermediatePage* parent,
    uint16_t index_in_parent,
    Page** page);
  /** subroutine to follow a pointer to head of bin from a volatile parent */
  ErrorCode   follow_page_bin_head(
    thread::Thread* context,
    bool for_write,
    HashIntermediatePage* parent,
    uint16_t index_in_parent,
    Page** page);

  /**
   * @brief Find a pointer to the bin that contains records for the hash.
   * @param[in] context Thread context
   * @param[in] for_write Whether we are reading these pages to modify
   * @param[in] combo Hash values.
   * @param[out] bin_head Pointer to the first data page of the bin. Might be null.
   * @details
   * If the search is for-write search, we always create a volatile page, even recursively,
   * thus *bin_head!= null.
   *
   * If the search is a for-read search and also the corresponding data page or its ascendants
   * do not exist yet, *bin_head returns null.
   * In that case, you can just return "not found" as a result.
   * locate_bin() internally adds a pointer set to protect the result, too.
   *
   * @note Although the above rule is simple, this might be wasteful in some situation.
   * Deletions and updates for non-existing keys do not need to instantiate volatile pages.
   * It can be handled like read access in that case.
   * But, to really achieve it, delete/update have to be 2-path. Search it using snapshot pages,
   * then goes through volatile path when found.
   * Rather, let's always create volatile pages for all writes. It's simple and performs
   * the best except a very unlucky case. If miss-delete/update is really the common path,
   * the user code can manually achieve the same thing by get() first, then delete/update().
   */
  ErrorCode   locate_bin(
    thread::Thread* context,
    bool for_write,
    const HashCombo& combo,
    HashDataPage** bin_head);

  /** return value of locate_record() */
  struct RecordLocation {
    /** Address of the slot. null if not found. */
    HashDataPage::Slot* slot_;
    /** Address of the record. null if not found. */
    char* record_;
    /**
     * TID as of locate_record() identifying the record.
     * guaranteed to be not is_moved (then automatically retried), though the \b current
     * TID might be now moved, in which case pre-commit will catch it.
     */
    xct::XctId observed_;

    void clear() {
      slot_ = nullptr;
      record_ = nullptr;
      observed_.data_ = 0;
    }
  };

  /**
   * @brief Usually follows locate_bin to locate the exact physical record for the key, or
   * create a new one if not exists (only when for_write).
   * @param[in] context Thread context
   * @param[in] for_write Whether we are seeking the record to modify
   * @param[in] create_if_notfound Whether we will create a new physical record if not exists
   * @param[in] create_payload_length If this method creates a physical record, it makes sure
   * the record can accomodate at least this size of payload.
   * @param[in] combo Hash values. Also the result of this method.
   * @param[in] bin_head Pointer to the first data page of the bin.
   * @param[out] result Information on the found slot.
   * @pre bin_head->get_bin() == combo.bin_
   * @pre bin_head != nullptr
   * @pre bin_head must be volatile page if for_write
   * @details
   * If the exact record is not found, this method protects the result by adding page version set.
   * Delete/update are just done in that case.
   * If create_if_notfound is true (an insert case), this method creates a new physical record for
   * the key with a deleted-state as a system transaction.
   */
  ErrorCode   locate_record(
    thread::Thread* context,
    bool for_write,
    bool create_if_notfound,
    uint16_t create_payload_length,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    HashDataPage* bin_head,
    RecordLocation* result);

  ErrorCode   reserve_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    uint16_t payload_length,
    HashDataPage* page,
    uint16_t examined_records,
    RecordLocation* result);

  /**
   * subroutine in locate_record() and reserve_record() to look for the key in one page.
   * result returns null pointers if not found, \b assuming the record_count.
   */
  void search_key_in_a_page(
    const void* key,
    uint16_t key_length,
    const HashCombo& combo,
    HashDataPage* page,
    uint16_t record_count,
    RecordLocation* result);
};
static_assert(sizeof(HashStoragePimpl) <= kPageSize, "HashStoragePimpl is too large");
static_assert(
  sizeof(HashStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "HashStorageControlBlock is too large.");
}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_
