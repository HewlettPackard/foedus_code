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
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_access.hpp"
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
  enum Constants {
    kMaxNodeSets = 256,
    kFrequencyHashSize = 256,
  };


  struct KickoutInfo {
      uint32_t add_count;  // incremented every time something is added to a bin, except for when
      // that thing needed a kickout
      uint32_t kickout_count;  // incremented every time something is kicked out from a bin.
      // Note: Since we know the size of the contents in the bin, these two are very similar.
      // If we really wanted to, we could possibly combine them to one thing
      KickoutInfo() {
        add_count = 0;
        kickout_count = 0;
      }
  };

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
    frequency_hash_.clear();
    schema_xct_ = schema_xct;
    isolation_level_ = isolation_level;
    node_set_size_ = 0;
    read_set_size_ = 0;
    write_set_size_ = 0;
    lock_free_write_set_size_ = 0;
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
  /** Returns if this transaction makes no writes. */
  bool                is_read_only() const {
    return write_set_size_ == 0 && lock_free_write_set_size_ == 0;
  }
  /** Returns the level of isolation for this transaction. */
  IsolationLevel      get_isolation_level() const { return isolation_level_; }
  /** Returns the ID of this transaction, but note that it is not issued until commit time! */
  const XctId&        get_id() const { return id_; }
  uint32_t            get_node_set_size() const { return node_set_size_; }
  uint32_t            get_read_set_size() const { return read_set_size_; }
  uint32_t            get_write_set_size() const { return write_set_size_; }
  uint32_t            get_lock_free_write_set_size() const { return lock_free_write_set_size_; }
  const NodeAccess*   get_node_set() const { return node_set_; }
  XctAccess*          get_read_set()  { return read_set_; }
  WriteXctAccess*     get_write_set() { return write_set_; }
  LockFreeWriteXctAccess* get_lock_free_write_set() { return lock_free_write_set_; }
  void add_frequency(uint32_t bin, uint32_t storageid, bool caused_kickout) {
    frequency_hash_.add(bin, storageid, caused_kickout);
  }
  KickoutInfo read_frequency(uint32_t bin, uint32_t storageid) {
    return frequency_hash_.read(bin, storageid);
  }



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
   * @brief Add the given page pointer to the node set of this transaction.
   * @details
   * You must call this method in the following cases;
   *  \li When following a volatile pointer that might be later swapped with the RCU protocol.
   *  \li When following a snapshot pointer except it is under a snapshot page.
   *
   * To clarify, the first case does not apply to storage types that don't swap volatile pointers.
   * So far, only \ref MASSTREE has such a swapping for page splits. All other storage types
   * thus don't have to take node sets for this.
   *
   * The second case doesn't apply to snapshot pointers once we follow a snapshot pointer in the
   * tree because everything is assured to be stable once we follow a snapshot pointer.
   *
   * Inlined in xct_inl.hpp.
   */
  ErrorCode           add_to_node_set(
    const storage::VolatilePagePointer* pointer_address,
    storage::VolatilePagePointer observed);

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
  ErrorCode           read_record(
    storage::Storage* storage,
    storage::Record* record,
    void *payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** read_record() for primitive types. */
  template <typename T>
  ErrorCode           read_record_primitive(
    storage::Storage* storage,
    storage::Record* record,
    T *payload,
    uint16_t payload_offset);

  /**
   * @brief Add the given record to the write set of this transaction.
   * @details
   * Inlined in xct_inl.hpp.
   */
  ErrorCode           add_to_write_set(
    storage::Storage* storage,
    storage::Record* record,
    log::RecordLogType* log_entry);

  /**
   * @brief Add the given log to the lock-free write set of this transaction.
   * @details
   * Inlined in xct_inl.hpp.
   */
  ErrorCode           add_to_lock_free_write_set(
    storage::Storage* storage,
    log::RecordLogType* log_entry);

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


  /**
   * @brief read(bin, storageid).add_count promises to be a number at least as great as
   * |inserts - deletions| run by the thread on the specified bin number in the specified
   * storage id. read(bin, storageid).kickout_count promises to be a number at least as
   * great as # kickouts at that bin so far by the thread.
   * @details
   * Since FrequencyHash hashes several bins from various storage ids to the same
   * hash-position, it will sometimes claim that the transaction has inserted more things into
   * a given bin than it actually has. So that we can use FrequencyHash in order to know when we
   * need to do a kickout, it is important that it never claims we have inserted fewer things into
   * a bin than we actually have. In order to accomplish this, we don't decrement counters in the
   * hash table except for deletions.
   *
   * If we are inserting something into a bin, we check if the sum of the relavent add_count
   * and the number of elements in the bin is too high. If not, we increment the add_count.
   * Otherwise, we increment kick_count which is used to determine who to kick out of the bin.
   *
   */

  struct FrequencyHash{
    KickoutInfo array_[kFrequencyHashSize];
    FrequencyHash() {  // Size must be a power of two
     clear();
    }
    uint32_t hash(uint32_t a) {  // Borrowed from Wang at
      // http://www.concentric.net/~ttwang/tech/inthash.htm
      // which no longer seems to be active link
      a = (a + 0x7ed55d16) + (a << 12);
      a = (a ^ 0xc761c23c) ^ (a >> 19);
      a = (a + 0x165667b1) + (a << 5);
      a = (a + 0xd3a2646c) ^ (a << 9);
      a = (a + 0xfd7046c5) + (a << 3);
      a = (a ^ 0xb55a4f09) ^ (a >> 16);
      return a;
    }
    void add(uint32_t bin, uint32_t storageid, bool caused_kickout) {
      uint32_t bucket = (hash(bin) ^ hash(storageid)) % kFrequencyHashSize;
      if (!caused_kickout) array_[bucket].add_count++;
      if (caused_kickout) array_[bucket].kickout_count++;
    }
    KickoutInfo read(uint32_t bin, uint32_t storageid) {
      uint32_t bucket = (hash(bin) ^ hash(storageid)) % kFrequencyHashSize;
      return array_[bucket];
    }
    void clear() {
      for (int x = 0; x < kFrequencyHashSize; x++) array_[x] = KickoutInfo();
    }
  };

  FrequencyHash frequency_hash_;

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

  LockFreeWriteXctAccess* lock_free_write_set_;
  uint32_t                lock_free_write_set_size_;
  uint32_t                max_lock_free_write_set_size_;

  // @todo we also need a special lock_free read set just for scanning xct on sequential storage.
  // it should check if the biggest XctId the scanner read is still the biggest XctId in the list.
  // we can easily implement it by remembering "safe" page to resume search, or just remembering
  // tail (abort if tail has changed), and then reading all record in the page.
  // as we don't have scanning accesses to sequential storage yet, low priority.

  // node set should be much smaller than others, so have it as an array.
  NodeAccess          node_set_[kMaxNodeSets];
  uint32_t            node_set_size_;

  /** @copydoc get_in_commit_log_epoch() */
  Epoch               in_commit_log_epoch_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_HPP_
