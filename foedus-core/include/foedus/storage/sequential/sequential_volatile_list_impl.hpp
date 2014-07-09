/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_VOLATILE_LIST_IMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_VOLATILE_LIST_IMPL_HPP_

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
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
  SequentialVolatileList() = delete;
  SequentialVolatileList(Engine* engine, StorageId storage_id);

  explicit SequentialVolatileList(const SequentialVolatileList& other) = delete;
  SequentialVolatileList& operator=(const SequentialVolatileList& other) = delete;

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  StorageId get_storage_id() const { return storage_id_; }
  thread::ThreadGlobalOrdinal get_thread_count() const { return thread_count_; }

  SequentialPage* get_head(thread::ThreadGlobalOrdinal ordinal) const {
    ASSERT_ND(ordinal < thread_count_);
    return head_pointers_[ordinal];
  }
  SequentialPage* get_tail(thread::ThreadGlobalOrdinal ordinal) const {
    ASSERT_ND(ordinal < thread_count_);
    return tail_pointers_[ordinal];
  }

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

  friend std::ostream& operator<<(std::ostream& o, const SequentialVolatileList& v);

 private:
  Engine* const     engine_;
  const StorageId   storage_id_;
  const thread::ThreadGlobalOrdinal thread_count_;

  /** Index is ThreadGlobalOrdinal.  */
  SequentialPage** head_pointers_;
  /** Index is ThreadGlobalOrdinal.  */
  SequentialPage** tail_pointers_;
};
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_VOLATILE_LIST_IMPL_HPP_
