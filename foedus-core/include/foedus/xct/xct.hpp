/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_HPP_
#define FOEDUS_XCT_XCT_HPP_
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief Represents a transaction.
 * @ingroup XCT
 * @details
 * To obtain this object, call Thread#get_current_xct().
 */
class Xct {
 public:
  Xct(Engine* engine, thread::ThreadId thread_id);

  // No copy
  Xct(const Xct& other) CXX11_FUNC_DELETE;
  Xct& operator=(const Xct& other) CXX11_FUNC_DELETE;

  void initialize(thread::ThreadId thread_id, memory::NumaCoreMemory* core_memory);

  /**
   * Begins the transaction.
   */
  void                activate(IsolationLevel isolation_level, bool schema_xct = false) {
    ASSERT_ND(!active_);
    active_ = true;
    schema_xct_ = schema_xct;
    isolation_level_ = isolation_level;
    read_set_size_ = 0;
    write_set_size_ = 0;
  }

  /**
   * Closes the transaction.
   */
  void                deactivate() {
    ASSERT_ND(active_);
    active_ = false;
  }

  /** Returns whether the object is an active transaction. */
  bool                is_active() const { return active_; }
  /**
   * Whether the transaction is a schema-modification transaction, which issues only
   * storage create/drop/alter etc operations.
   */
  bool                is_schema_xct() const { return schema_xct_; }
  /** Returns the level of isolation for this transaction. */
  IsolationLevel      get_isolation_level() const { return isolation_level_; }
  /** Returns the ID of this transaction, but note that it is not issued until commit time! */
  const XctId&        get_id() const { return id_; }
  uint32_t            get_read_set_size() const { return read_set_size_; }
  uint32_t            get_write_set_size() const { return write_set_size_; }
  XctAccess*          get_read_set()  { return read_set_; }
  WriteXctAccess*     get_write_set() { return write_set_; }

  /**
   * @brief Called while a successful commit of read-write or schema xct to issue a new xct id.
   * @param[in,out] epoch (in) The \e minimal epoch this transaction has to be in. (out)
   * the epoch this transaction ended up with, which is epoch+1 only when it found ordinal is
   * full for the current epoch.
   * @details
   * This method issues a XctId that satisfies the following properties (see [TU13]).
   * Clarification: "larger" hereby means either a) the epoch is larger or
   * b) the epoch is same and ordinal is larger.
   * \li Larger than the most recent XctId issued for read-write transaction on this thread.
   * \li Larger than every XctId of any record read or written by this transaction.
   * \li In the \e returned(out) epoch (which is same or larger than the given(in) epoch).
   *
   * This method also advancec epoch when ordinal is full for the current epoch.
   * This method never fails.
   */
  void                issue_next_id(Epoch *epoch);

  /**
   * @brief Add the given record to the read set of this transaction.
   * @details
   * You must call this method \b BEFORE reading the data, otherwise it violates the
   * commit protocol. This method takes an appropriate memory fence to prohibit local reordering,
   * but global staleness is fine (in other words, std::memory_order_consume rather
   * than std::memory_order_acquire, although both are no-op in x86 which is TSO...).
   * Inlined in xct_inl.hpp.
   */
  ErrorCode           add_to_read_set(storage::Storage* storage, storage::Record* record);

  /** add_to_read_set() plus the data read plus version check again. */
  ErrorCode           read_record(storage::Storage* storage, storage::Record* record,
                void *payload, uint16_t payload_offset, uint16_t payload_count);
  /** read_record() for primitive types. */
  template <typename T>
  ErrorCode           read_record_primitive(storage::Storage* storage, storage::Record* record,
                T *payload, uint16_t payload_offset);

  /**
   * @brief Add the given record to the write set of this transaction.
   * @details
   * Inlined in xct_inl.hpp.
   */
  ErrorCode           add_to_write_set(storage::Storage* storage, storage::Record* record,
                     void* log_entry);

  /**
   * @brief If this transaction is currently committing with some log to publish, this
   * gives the \e conservative estimate (although usually exact) of the commit epoch.
   * @details
   * This is used by loggers to tell if it can assume that this transaction already got a new
   * epoch or not in commit phase. If it's not the case, the logger will spin on this until
   * this returns 0 or epoch that is enough recent. Without this mechanisim, we will get a too
   * conservative value of "min(ctid_w)" (Sec 4.10 [TU2013]) when there are some threads that
   * are either idle or spending long time before/after commit.
   *
   * The transaction takes an appropriate fence before updating this value so that
   * followings are guaranteed:
   * \li When this returns 0, this transaction will not publish any more log without getting
   * recent epoch (see destructor of InCommitLogEpochGuard).
   * \li If this returns epoch-X, the transaction will never publishe a log whose epoch is less
   * than X. (this is assured by taking InCommitLogEpochGuard BEFORE the first fence in commit)
   * \li As an added guarantee, this value will be updated as soon as the commit phase ends, so
   * the logger can safely spin on this value.
   *
   * @note A similar protocol seems implemented in MIT Silo, too. See
   * how "txn_logger::advance_system_sync_epoch" updates per_thread_sync_epochs_ and
   * system_sync_epoch_. However, not quite sure about their implementation. Will ask.
   * @see InCommitLogEpochGuard
   */
  Epoch               get_in_commit_log_epoch() const {
    assorted::memory_fence_acquire();
    return in_commit_log_epoch_;
  }

  /**
   * Automatically resets in_commit_log_epoch_ with appropriate fence.
   * This guards the range from a read-write transaction starts committing until it publishes
   * or discards the logs.
   * @see get_in_commit_log_epoch()
   * @see foedus::xct::XctManagerPimpl::precommit_xct_readwrite()
   */
  struct InCommitLogEpochGuard {
    InCommitLogEpochGuard(Xct *xct, Epoch current_epoch) : xct_(xct) {
      xct_->in_commit_log_epoch_ = current_epoch;
    }
    ~InCommitLogEpochGuard() {
      // prohibit reorder the change on ThreadLogBuffer#offset_committed_
      // BEFORE update to in_commit_log_epoch_. This is to satisfy the first requirement:
      // ("When this returns 0, this transaction will not publish any more log without getting
      // recent epoch").
      // Without this fence, logger can potentially miss the log that has been just published
      // with the old epoch.
      assorted::memory_fence_release();
      xct_->in_commit_log_epoch_ = Epoch(0);
      // We can also call another memory_order_release here to immediately publish it,
      // but it's anyway rare. The spinning logger will eventually get the update, so no need.
      // In non-TSO architecture, this also saves some overhead in critical path.
    }
    Xct* const xct_;
  };

  friend std::ostream& operator<<(std::ostream& o, const Xct& v);

 private:
  Engine* const engine_;

  /** Thread that owns this transaction. */
  const thread::ThreadId thread_id_;

  /**
   * Most recently issued ID of this transaction. XctID is issued at commit time,
   * so this is "previous" ID unless while or right after commit.
   */
  XctId               id_;

  /** Level of isolation for this transaction. */
  IsolationLevel      isolation_level_;

  /** Whether the object is an active transaction. */
  bool                active_;

  /**
   * Whether the transaction is a schema-modification transaction, which issues only
   * storage create/drop/alter etc operations.
   */
  bool                schema_xct_;

  XctAccess*          read_set_;
  uint32_t            read_set_size_;
  uint32_t            max_read_set_size_;

  WriteXctAccess*     write_set_;
  uint32_t            write_set_size_;
  uint32_t            max_write_set_size_;

  /** @copydoc get_in_commit_log_epoch() */
  Epoch               in_commit_log_epoch_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_HPP_
