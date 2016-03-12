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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <mutex>
#include <string>
#include <vector>

#include "foedus/attachable.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/** Shared data of this storage type */
struct MasstreeStorageControlBlock final {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  MasstreeStorageControlBlock() = delete;
  ~MasstreeStorageControlBlock() = delete;

  bool exists() const { return status_ == kExists || status_ == kMarkedForDeath; }

  soc::SharedMutex    status_mutex_;
  /** Status of the storage */
  StorageStatus       status_;
  /**
   * Points to the root page (or something equivalent).
   * Masstree-specific:
   * Volatile pointer is always active.
   * This is always MasstreeIntermediatePage (a contract only for first layer's root page).
   * When the first layer B-tree grows, this points to a new page. So, this is one of the few
   * page pointers that might be \e swapped. Transactions thus have to add this to a pointer
   * set even thought they are following a volatile pointer.
   *
   * Instead, this always points to a root. We don't need "is_root" check in [YANDONG12] and
   * thus doesn't need a parent pointer.
   */
  DualPagePointer     root_page_pointer_;
  /** metadata of this storage. */
  MasstreeMetadata    meta_;

  // Do NOT reorder members up to here. The layout must be compatible with StorageControlBlock
  // Type-specific shared members below.

  /**
   * Lock to synchronize updates to root_page_pointer_.
   * This lock must be one that can be used outside of volatile page, i.e., a dumb spinlock
   * or MCS Lock because we put it in the control block. Locks that require UniversalLockId
   * conversion can't be used here, e.g., the Extended RW Lock. We have also separate grow_root
   * functions to deal with this issue.
   */
  xct::LockableXctId  first_root_owner_;
};

/**
 * @brief Pimpl object of MasstreeStorage.
 * @ingroup MASSTREE
 * @details
 * A private pimpl object for MasstreeStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class MasstreeStoragePimpl final : public Attachable<MasstreeStorageControlBlock> {
 public:
  MasstreeStoragePimpl() : Attachable<MasstreeStorageControlBlock>() {}
  explicit MasstreeStoragePimpl(MasstreeStorage* storage)
    : Attachable<MasstreeStorageControlBlock>(
      storage->get_engine(),
      storage->get_control_block()) {}

  ErrorStack  create(const MasstreeMetadata& metadata);
  ErrorStack  load(const StorageControlBlock& snapshot_block);
  ErrorStack  load_empty();
  ErrorStack  drop();

  bool                exists()    const { return control_block_->exists(); }
  StorageId           get_id()    const { return control_block_->meta_.id_; }
  const StorageName&  get_name()  const { return control_block_->meta_.name_; }
  const MasstreeMetadata& get_meta()  const { return control_block_->meta_; }
  DualPagePointer& get_first_root_pointer() { return control_block_->root_page_pointer_; }
  DualPagePointer* get_first_root_pointer_address() { return &control_block_->root_page_pointer_; }
  xct::LockableXctId& get_first_root_owner() { return control_block_->first_root_owner_; }

  ErrorCode get_first_root(
    thread::Thread* context,
    bool for_write,
    MasstreeIntermediatePage** root);

  /**
   * Different grow_*_root functions for first-root and non-first-root locks.
   * Note the different types of root_pointer_owner in grow_first_root and grow_non_first_root.
   * Both functions calls grow_root to do the bulk of their work.
   */
  ErrorCode grow_first_root(
    thread::Thread* context,
    DualPagePointer* root_pointer,
    xct::LockableXctId* root_pointer_owner,
    MasstreeIntermediatePage** new_root);
  ErrorCode grow_non_first_root(
    thread::Thread* context,
    DualPagePointer* root_pointer,
    xct::RwLockableXctId* root_pointer_owner,
    MasstreeIntermediatePage** new_root);
  ErrorCode grow_root(
    thread::Thread* context,
    DualPagePointer* root_pointer,
    MasstreeIntermediatePage** new_root);

  /**
   * Find a border node in the layer that corresponds to the given key slice.
   * @note this method is \e physical-only. It doesn't take any long-term lock or readset.
   */
  ErrorCode find_border_physical(
    thread::Thread* context,
    MasstreePage* layer_root,
    uint8_t   current_layer,
    bool      for_writes,
    KeySlice  slice,
    MasstreeBorderPage** border) ALWAYS_INLINE;

  /** Identifies page and record for the key */
  ErrorCode locate_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    bool for_writes,
    RecordLocation* result);
  /** Identifies page and record for the normalized key */
  ErrorCode locate_record_normalized(
    thread::Thread* context,
    KeySlice key,
    bool for_writes,
    RecordLocation* result);

  /**
   * @brief Runs a system transaction to expand the record space,
   * @param[in] context Thread context
   * @param[in] physical_payload_hint Minimal payload size we want to guarantee
   * @param[in,out] border The page that contains the record
   * @param[in] record_index The record to expand
   * @param[in,out] lock_scope Already-acquired lock on the border page
   * @pre border->is_locked() by this thread. We receive lock_scope just to enforce that.
   * @pre !border->is_moved(), so we can expand records or split this page.
   * @pre border->get_max_payload_length(record_index) < payload_count. Otherwise why called.
   * @pre !border->does_point_to_layer(record_index).
   * @details
   * This method tries 3 things to expand the record space, in preference order:
   * \li If the record is currently placed at the right-most record region
   * (or, page's next_offset == record's offset+len), we can just change the length field.
   * We don't even need a record-lock in this case.
   * \li If the page has enough space, we allocate a new record region and changes offset/length
   * fields. We have to take a record-lock and increments TID so that concurrent threads
   * detect the change at precommit.
   * \li If none of the above works, we split this page and expand the record in the new page.
   * When this method returns without an error, it is guaranteed that
   * there are new foster children with the expanded record.
   * Internally, it might not be enough to split just once
   * to reserve a record with the given size when it is large. In that case tbis method recursively
   * splits the page.
   */
  ErrorCode expand_record(
    thread::Thread* context,
    PayloadLength physical_payload_hint,
    MasstreeBorderPage* border,
    SlotIndex record_index,
    PageVersionLockScope* lock_scope);
  /**
   * locks the page, invokes expand_record(), then releases the lock.
   * When, after locking, it turns out that we can't expand, it does nothing (not an error).
   * Hence, in either case, the caller must retry.
   */
  ErrorCode lock_and_expand_record(
    thread::Thread* context,
    PayloadLength required_payload_count,
    MasstreeBorderPage* border,
    SlotIndex record_index);

  /**
   * Like locate_record(), this is also a logical operation.
   */
  ErrorCode reserve_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    PayloadLength payload_count,
    PayloadLength physical_payload_hint,
    RecordLocation* result);
  ErrorCode reserve_record_normalized(
    thread::Thread* context,
    KeySlice key,
    PayloadLength payload_count,
    PayloadLength physical_payload_hint,
    RecordLocation* result);

  /**
   * @brief A sub-routine of reserve_record()/reserve_record_normalized() to allocate a new record
   * in the given page.
   * @pre border->is_locked() && !border->is_moved()
   * @pre !out_page_lock->released_ && border->get_version_address() == out_page_lock->version_
   * meaning out_page_lock is locking \b border \b initially.
   * @post !out_page_lock->released_ && *out_page->get_version_address() == out_page_lock->version_
   * meaning out_page_lock is locking \b out_page (which may or may not be border) \b afterwards.
   * @post *out_page->is_locked() && !*out_page->is_moved()
   * @details
   * This function might internally splits the border page to make room.
   * out_page returns the page to which the new record landed, which might be border or
   * its foster-child if page split happens.
   * The page split might recurse if one split is not enough to make room.
   * In that case, out_page might be foster-grandchild or even deeper although that should
   * happen pretty rarely.
   */
  ErrorCode reserve_record_new_record(
    thread::Thread* context,
    MasstreeBorderPage* border,
    KeySlice key,
    KeyLength remainder,
    const void* suffix,
    PayloadLength payload_count,
    PageVersionLockScope* out_page_lock,
    RecordLocation* result);

  ErrorCode reserve_record_new_record_apply(
    thread::Thread* context,
    MasstreeBorderPage* target,
    SlotIndex target_index,
    KeySlice slice,
    KeyLength remainder_length,
    const void* suffix,
    PayloadLength payload_count,
    RecordLocation* result);
  /**
   * Subroutine of reserve_record() to create an initially next-layer record in border and
   * returns the new root page of the new layer.
   * @pre border must be locked by this thread
   * @pre !border->is_moved()
   */
  ErrorCode reserve_record_next_layer(
    thread::Thread* context,
    MasstreeBorderPage* border,
    KeySlice slice,
    MasstreePage** out_page);
  ErrorCode reserve_record_next_layer_apply(
    thread::Thread* context,
    MasstreeBorderPage* target,
    KeySlice slice,
    MasstreePage** out_page);

  /** implementation of get_record family. use with locate_record() */
  ErrorCode retrieve_general(
    thread::Thread* context,
    const RecordLocation& location,
    void* payload,
    PayloadLength* payload_capacity);
  ErrorCode retrieve_part_general(
    thread::Thread* context,
    const RecordLocation& location,
    void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count);

  /** Used in the following methods */
  ErrorCode register_record_write_log(
    thread::Thread* context,
    const RecordLocation& location,
    log::RecordLogType* log_entry);

  /** implementation of insert_record family. use with \b reserve_record() */
  ErrorCode insert_general(
    thread::Thread* context,
    const RecordLocation& location,
    const void* be_key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_count);

  /** implementation of delete_record family. use with locate_record()  */
  ErrorCode delete_general(
    thread::Thread* context,
    const RecordLocation& location,
    const void* be_key,
    KeyLength key_length);

  /** implementation of upsert_record family. use with \b reserve_record() */
  ErrorCode upsert_general(
    thread::Thread* context,
    const RecordLocation& location,
    const void* be_key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_count);

  /** implementation of overwrite_record family. use with locate_record()  */
  ErrorCode overwrite_general(
    thread::Thread* context,
    const RecordLocation& location,
    const void* be_key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count);

  /** implementation of increment_record family. use with locate_record()  */
  template <typename PAYLOAD>
  ErrorCode increment_general(
    thread::Thread* context,
    const RecordLocation& location,
    const void* be_key,
    KeyLength key_length,
    PAYLOAD* value,
    PayloadLength payload_offset);

  /** These are defined in masstree_storage_verify.cpp */
  ErrorStack verify_single_thread(thread::Thread* context);
  ErrorStack verify_single_thread_layer(
    thread::Thread* context,
    uint8_t layer,
    MasstreePage* layer_root);
  ErrorStack verify_single_thread_intermediate(
    thread::Thread* context,
    KeySlice low_fence,
    HighFence high_fence,
    MasstreeIntermediatePage* page);
  ErrorStack verify_single_thread_border(
    thread::Thread* context,
    KeySlice low_fence,
    HighFence high_fence,
    MasstreeBorderPage* page);

  /** These are defined in masstree_storage_debug.cpp */
  ErrorStack debugout_single_thread(
    Engine* engine,
    bool volatile_only,
    uint32_t max_pages);
  ErrorStack debugout_single_thread_recurse(
    Engine* engine,
    cache::SnapshotFileSet* fileset,
    MasstreePage* parent,
    bool follow_volatile,
    uint32_t* remaining_pages);
  ErrorStack debugout_single_thread_follow(
    Engine* engine,
    cache::SnapshotFileSet* fileset,
    const DualPagePointer& pointer,
    bool follow_volatile,
    uint32_t* remaining_pages);

  /** For stupid reasons (I'm lazy!) these are defined in _debug.cpp. */
  ErrorStack  hcc_reset_all_temperature_stat(Engine* engine);
  ErrorStack  hcc_reset_all_temperature_stat_recurse(Engine* engine, MasstreePage* parent);
  ErrorStack  hcc_reset_all_temperature_stat_follow(Engine* engine, VolatilePagePointer page_id);

  /** Thread::follow_page_pointer() for masstree */
  ErrorCode follow_page(
    thread::Thread* context,
    bool for_writes,
    storage::DualPagePointer* pointer,
    MasstreePage** page);
  /** Follows to next layer's root page. */
  ErrorCode follow_layer(
    thread::Thread* context,
    bool for_writes,
    MasstreeBorderPage* parent,
    SlotIndex record_index,
    MasstreePage** page) ALWAYS_INLINE;

  /**
   * Reserve a next layer as one system transaction.
   * parent may or maynot be locked.
   */
  ErrorCode create_next_layer(
    thread::Thread* context,
    MasstreeBorderPage* parent,
    SlotIndex parent_index);

  /** defined in masstree_storage_prefetch.cpp */
  ErrorCode prefetch_pages_normalized(
    thread::Thread* context,
    bool install_volatile,
    bool cache_snapshot,
    KeySlice from,
    KeySlice to);
  ErrorCode prefetch_pages_normalized_recurse(
    thread::Thread* context,
    bool install_volatile,
    bool cache_snapshot,
    KeySlice from,
    KeySlice to,
    MasstreePage* page);
  ErrorCode prefetch_pages_follow(
    thread::Thread* context,
    DualPagePointer* pointer,
    bool vol_on,
    bool snp_on,
    KeySlice from,
    KeySlice to);

  xct::TrackMovedRecordResult track_moved_record(
    xct::RwLockableXctId* old_address,
    xct::WriteXctAccess* write_set) ALWAYS_INLINE;

  /** Defined in masstree_storage_peek.cpp */
  ErrorCode     peek_volatile_page_boundaries(
    Engine* engine,
    const MasstreeStorage::PeekBoundariesArguments& args);
  ErrorCode     peek_volatile_page_boundaries_next_layer(
    const MasstreePage* layer_root,
    const memory::GlobalVolatilePageResolver& resolver,
    const MasstreeStorage::PeekBoundariesArguments& args);
  ErrorCode     peek_volatile_page_boundaries_this_layer(
    const MasstreePage* layer_root,
    const memory::GlobalVolatilePageResolver& resolver,
    const MasstreeStorage::PeekBoundariesArguments& args);
  ErrorCode     peek_volatile_page_boundaries_this_layer_recurse(
    const MasstreeIntermediatePage* cur,
    const memory::GlobalVolatilePageResolver& resolver,
    const MasstreeStorage::PeekBoundariesArguments& args);

  /** Defined in masstree_storage_fatify.cpp */
  ErrorStack    fatify_first_root(thread::Thread* context, uint32_t desired_count);
  ErrorStack    fatify_first_root_double(thread::Thread* context);

  static ErrorCode check_next_layer_bit(xct::XctId observed) ALWAYS_INLINE;
};
static_assert(sizeof(MasstreeStoragePimpl) <= kPageSize, "MasstreeStoragePimpl is too large");
static_assert(
  sizeof(MasstreeStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "MasstreeStorageControlBlock is too large.");
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
