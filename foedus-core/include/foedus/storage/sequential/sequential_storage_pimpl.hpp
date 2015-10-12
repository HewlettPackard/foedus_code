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
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <atomic>

#include "foedus/assert_nd.hpp"
#include "foedus/attachable.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/fwd.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/** Shared data of this storage type */
struct SequentialStorageControlBlock final {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  SequentialStorageControlBlock() = delete;
  ~SequentialStorageControlBlock() = delete;

  bool exists() const { return status_ == kExists || status_ == kMarkedForDeath; }
  ErrorCode optimistic_read_truncate_epoch(thread::Thread* context, Epoch* out) const;

  soc::SharedMutex    status_mutex_;
  /** Status of the storage */
  StorageStatus       status_;
  /** Points to the root page (or something equivalent). */
  DualPagePointer     root_page_pointer_;
  /** metadata of this storage. */
  SequentialMetadata  meta_;

  // Do NOT reorder members up to here. The layout must be compatible with StorageControlBlock
  // Type-specific shared members below.

  /**
   * Protects accesses to cur_truncate_epoch_.
   * \li Append-operations don't need to check it.
   * \li Scan-operations take this as a read-set.
   * \li Truncate-operation locks it and update cur_truncate_epoch_.
   */
  xct::RwLockableXctId  cur_truncate_epoch_tid_;

  /** @copydoc foedus::storage::sequential::SequentialMetadata::truncate_epoch_ */
  std::atomic< Epoch::EpochInteger >  cur_truncate_epoch_;

  /**
   * Points to pages that store thread-private head pages to store thread-private volatile pages.
   * Each page can contain 2^10 pointers (as the node is implicit, PagePoolOffset suffices)
   * and we can have at most 2^16 cores. Thus we have 2^6 pointers here.
   * This means we can waste 64*2=128 volatile pages (=512kb) per one sequential storage..
   * shouldn't be a big issue.
   */
  VolatilePagePointer   head_pointer_pages_[kPointerPageCount];
  /** Same above, but for tail pointers. */
  VolatilePagePointer   tail_pointer_pages_[kPointerPageCount];
};

/**
 * @brief Lock-free list of records stored in the volatile part of sequential storage.
 * @ingroup SEQUENTIAL
 * @details
 * Volatile parts of sequential storages are completely separated from stable snapshot pages
 * and maintained as a \e lock-free list of SequentialPage.
 *
 * The append of record in page involves one atomic CAS, and append of a new page involves
 * one atomic CAS too. Because this list is an append/scan only list, this is way simpler
 * than typical lock-free lists.
 *
 * @section SEQ_VOL_LIST_CONCUR Concurrency Control
 * In this volatile list, we assume the following things to simplify concurrency control:
 *
 *  \li Each SequentialPage contains records only in one epoch. When epoch switches,
 * we insert new records to new page even if the page is almost vacant.
 *  \li The order in page does not necessarily reflect serialization order. The sequential storage
 * provides a \e set semantics rather than \e list semantics in terms of serializability
 * although it's loosely ordered.
 *  \li Each thread maintains its own head/tail pages so that they don't interfere each other
 * at all. This, combined with the assumption above, makes it completely without blocking
 * and atomic operations \b EXCEPT:
 *  \li For scanning cursors (which only rarely occur and are fundamentally slow anyways),
 * we take an exclusive lock when they touch \e unsafe epochs.
 * Further, the scanning thread must wait until all other threads
 * did NOT start before the scanning thread take lock. (otherwise serializability is not
 * guaranteed). We will have something like Xct::InCommitEpochGuard for this purpose.
 * As scanning threads are rare, they can wait for a while, so it's okay for other threads
 * to complete at least one transacion before they get aware of the lock.
 *  \li However, the above requirement is not mandatory if the scanning threads are running in
 * snapshot mode or dirty-read mode. We just make sure that what is reads is within the
 * epoch. Because other threads append record in serialization order to their own lists,
 * this can be trivially achieved.
 *
 * With these assumptions, sequential storages don't require any locking for serializability.
 * Thus, we separate write-sets of sequential storages from other write-sets in transaction objects.
 *
 *
 * @par Page replacement policy after snapshot
 * This is an append-optimized storage, so there is no point to keep volatile pages for future use.
 * We thus drop all volatile pages that were snapshot.
 *
 * @note
 * This is a private implementation-details of \ref SEQUENTIAL, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 * @todo Implement the scanning functionality as above.
 */
class SequentialStoragePimpl final : public Attachable<SequentialStorageControlBlock> {
 public:
  struct PointerPage {
    memory::PagePoolOffset pointers_[kPointersPerPage];
  };
  SequentialStoragePimpl() = delete;
  explicit SequentialStoragePimpl(SequentialStorage* storage)
    : Attachable<SequentialStorageControlBlock>(
      storage->get_engine(),
      storage->get_control_block()) {
  }
  SequentialStoragePimpl(Engine* engine, SequentialStorageControlBlock* control_block)
    : Attachable<SequentialStorageControlBlock>(engine, control_block) {
  }

  bool        exists() const { return control_block_->exists(); }
  StorageId   get_id() const { return control_block_->meta_.id_; }
  const StorageName& get_name() const { return control_block_->meta_.name_; }
  ErrorStack  create(const SequentialMetadata& metadata);
  ErrorStack  load(const StorageControlBlock& snapshot_block);
  ErrorStack  initialize_head_tail_pages();
  ErrorStack  drop();
  ErrorStack  truncate(Epoch new_truncate_epoch, Epoch* commit_epoch);
  void        apply_truncate(const SequentialTruncateLogType& the_log);

  SequentialPage* get_head(
    const memory::LocalPageResolver& resolver,
    thread::ThreadId thread_id) const;
  SequentialPage* get_tail(
    const memory::LocalPageResolver& resolver,
    thread::ThreadId thread_id) const;

  memory::PagePoolOffset* get_head_pointer(thread::ThreadId thread_id) const;
  memory::PagePoolOffset* get_tail_pointer(thread::ThreadId thread_id) const;


  /**
   * @brief Appends an already-commited record to this volatile list.
   * @details
   * This method is guaranteed to succeed, so it does not return error code,
   * which is essential to be used after commit.
   * Actually, there is one very rare case this method might fail: we need a new page
   * and the page pool has zero free page. However, we can trivially avoid this case
   * by checking if we have at least one free page in \e thread-local cache during pre-commit.
   */
  void        append_record(
    thread::Thread* context,
    xct::XctId owner_id,
    const void *payload,
    uint16_t payload_count);

  /**
   * @brief Traverse all pages and call back the handler for every page.
   * @details
   * Handler must look like "ErrorCode func(SequentialPage* page) { ... } ".
   */
  template <typename HANDLER>
  ErrorCode for_every_page(HANDLER handler) const {
    uint16_t nodes = engine_->get_options().thread_.group_count_;
    uint16_t threads_per_node = engine_->get_options().thread_.thread_count_per_group_;
    for (uint16_t node = 0; node < nodes; ++node) {
      const memory::LocalPageResolver& resolver
        = engine_->get_memory_manager()->get_node_memory(node)->get_volatile_pool()->get_resolver();
      for (uint16_t local_ordinal = 0; local_ordinal < threads_per_node; ++local_ordinal) {
        thread::ThreadId thread_id = thread::compose_thread_id(node, local_ordinal);
        for (SequentialPage* page = get_head(resolver, thread_id); page;) {
          ASSERT_ND(page->header().page_id_);
          CHECK_ERROR_CODE(handler(page));

          VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
          memory::PagePoolOffset offset = next_pointer.components.offset;
          if (offset != 0) {
            page = reinterpret_cast<SequentialPage*>(resolver.resolve_offset(offset));
          } else {
            page = nullptr;
          }
        }
      }
    }
    return kErrorCodeOk;
  }
};

static_assert(sizeof(SequentialStoragePimpl) <= kPageSize, "SequentialStoragePimpl is too large");
static_assert(
  sizeof(SequentialStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "SequentialStorageControlBlock is too large.");
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
