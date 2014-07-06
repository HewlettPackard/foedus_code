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
 *  \li There is at most one thread that \e compacts the volatile list; the snapshot thread.
 *
 * With these assumptions, sequential storages don't require any locking for serializability.
 * Thus, we separate write-sets of sequential storages from other write-sets in transaction objects.
 *
 * @note
 * This is a private implementation-details of \ref SEQUENTIAL, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class SequentialVolatileList final : public DefaultInitializable {
 public:
  SequentialVolatileList() = delete;
  SequentialVolatileList(Engine* engine, StorageId storage_id)
    : engine_(engine), storage_id_(storage_id), head_(nullptr), tail_(nullptr) {}

  explicit SequentialVolatileList(const SequentialVolatileList& other) = delete;
  SequentialVolatileList& operator=(const SequentialVolatileList& other) = delete;

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  SequentialPage*   get_head() const { return head_; }
  SequentialPage*   get_tail() const { return tail_; }

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
  SequentialPage*   head_;
  SequentialPage*   tail_;
};
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_VOLATILE_LIST_IMPL_HPP_
