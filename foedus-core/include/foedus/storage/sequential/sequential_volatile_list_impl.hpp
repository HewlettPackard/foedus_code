/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_VOLATILE_LIST_IMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_VOLATILE_LIST_IMPL_HPP_

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace sequential {
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
 *  \li For scanning threads (which only rarely occur and are fundamentally slow anyways),
 * we take an exclusive lock. Further, the scanning thread must wait until all other threads
 * did NOT start before the scanning thread take lock. (otherwise serializability is not
 * guaranteed). We will have something like Xct::InCommitLogEpochGuard for this purpose.
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
 * @note
 * This is a private implementation-details of \ref SEQUENTIAL, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 * @todo Implement the scanning functionality as above.
 */
class SequentialVolatileList final : public DefaultInitializable {
 public:
  struct PointerPage {
    memory::PagePoolOffset pointers_[kPointersPerPage];
  };
  SequentialVolatileList() = delete;
  SequentialVolatileList(Engine* engine, StorageId storage_id);

  explicit SequentialVolatileList(const SequentialVolatileList& other) = delete;
  SequentialVolatileList& operator=(const SequentialVolatileList& other) = delete;

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  StorageId get_storage_id() const { return storage_id_; }

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
      if (head_pointer_pages_cache_[node / 4] == nullptr) {
        continue;  // in case it's not initialized yet
      }
      const memory::LocalPageResolver& resolver
        = engine_->get_memory_manager().get_node_memory(node)->get_volatile_pool().get_resolver();
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

  friend std::ostream& operator<<(std::ostream& o, const SequentialVolatileList& v);

 private:
  Engine* const     engine_;
  const StorageId   storage_id_;

  /** @see get_pointer_page_and_index() */
  VolatilePagePointer head_pointer_pages_[kPointerPageCount];
  /** @see get_pointer_page_and_index() */
  VolatilePagePointer tail_pointer_pages_[kPointerPageCount];

  // these are local and read-only cache. we can safely do this because pointer pages
  // are never replaced.
  PointerPage*        head_pointer_pages_cache_[kPointerPageCount];
  PointerPage*        tail_pointer_pages_cache_[kPointerPageCount];
};
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_VOLATILE_LIST_IMPL_HPP_
