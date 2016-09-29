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
#ifndef FOEDUS_XCT_RETROSPECTIVE_LOCK_LIST_HPP_
#define FOEDUS_XCT_RETROSPECTIVE_LOCK_LIST_HPP_

#include <stdint.h>

#include <algorithm>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/error_code.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief \b Retrospective \b Lock \b List (\b RLL) to avoid deadlocks.
 * @defgroup RLL Retrospective Lock List
 * @ingroup XCT
 * @details
 * @par What it is
 * Retrospective Lock List (\b RLL) is a thread-private list of locks the thread took or tried to
 * take in a previous transaction on the same thread. It's used when the previous transaction
 * aborted due to a read-conflict (OCC verification failure) or a lock-conflict (giving up acquiring
 * a write-lock for possible deadlocks).
 * The thread uses the list to pro-actively take locks in a \b canonical-mode
 * during the next transaction, expecting that
 * the next transaction will need almost the same accesses.
 * This protocol rules out deadlocks, thus dramatically improves concurrency for highly contended
 * workload. It's also very simple and scalable on many-cores.
 *
 * @par Canonical Mode
 * When a thread is trying to acquire a lock,
 * the thread is said to be in a \b Canonical \b Mode if and only if
 * all locks the thread has already taken, either read-lock or write-lock, are
 * \b universally-ordered before the lock the thread is trying to take.
 *
 * @par Universal Order of locks
 * We can use arbitrary ordering scheme as far as it's universally consistent.
 * For example, we can just order by the address of locks (except, we need to be a bit careful
 * on shared memory and virtual addresses). In the example,
 * a lock X is said to be ordered before another lock Y if and only if addr(X) < addr(Y).
 *
 * @par Benefits of Canonical Mode
 * When one is taking a lock in canonical mode,
 * the lock-acquire is guaranteed to be \b deadlock-free, thus the thread can simply
 * wait for the lock unconditionally.
 *
 * @par How we keep Canonical Mode as much as possible
 * We do a couple of things to keep the transaction in canonical mode as much as possible:
 * \li Following to the SILO protocol, we always acquire
 * write-locks during pre-commit in a universal order.
 * As far as we don't take read-locks (pure OCC), then we are always in canonical mode.
 * \li When we want to take a read-lock in a retry of a transaction, we use
 * the retrospective list to take locks who are ordered before the lock the thread is taking.
 * We keep doing this until the end of transaction.
 * \li When we find ourselves risking deadlocks during pre-commit, we \e might release some of
 * the locks we have taken so that we go back to canonical mode. This is done in best-effort.
 *
 * @par When Canonical Mode is violated
 * Initially, every thread is trivially in canonical mode because it has taken zero locks.
 * The thread goes out of canonical mode over time for various reasons.
 * \li The thread has no retrospective lock list, probably because it's the first trial of the xct.
 * \li The thread has used the retrospective lock list, but the thread
 * for some reason accessed different physical pages/records due to page splits, concurrent
 * updates, and many other reasons.
 * \li The thread needs to take a page-lock for page-split etc. In this case, to avoid deadlock,
 * we might have to discard retrospective locks.
 * \li Despite the best-effort retry to keep the canonical mode, there always is a chance that
 * we better give up. This case most likely resulting in aborts and just leaving retrospective list
 * for next trial.
 *
 * When this happens, still fine, it's not a correctness issue. It just goes back to
 * conditional locking mode with potential timeout, then even in the worst case aborts
 * and leaves the RLL for next run.
 *
 * What if the thread keeps issuing locks in very different
 * orders and it needs to take read-locks for avoiding verification failure?
 * Umm, then this scheme might keep aborting, but I can't think of any existing scheme that
 * works well for such a case either... except being VERY pessimisitic (take table lock!).
 *
 * @par Ohter uses of RLL
 * We can also use RLL to guide our decision to take read-locks or not.
 * If we got an OCC verification failure, we leave an entry in RLL and take a read-lock
 * in the next run. Or, potentially, we can leave an opposite note saying we probably don't
 * need a read-lock on the record. In many cases the per-page stat is enough to guide us,
 * but this is one option.
 */


/**
 * @brief An entry in CLL and RLL, representing a lock that is taken or will be taken.
 * @ingroup RLL
 * @details
 * This is a POD, and guaranteed to be init-ed/reset-ed by memzero and copied via memcpy.
 * We initially had a separate entry object for RLL and CLL, but they were always the same.
 * We might differentiate them later, but for now reuse the same code.
 */
struct LockEntry {
  /**
   * Used to order locks in canonical order.
   * So far universal_lock_id_ == reinterpret_cast<uintptr_t>(lock_).
   */
  UniversalLockId universal_lock_id_;

  /**
   * Virtual address of the lock.
   */
  RwLockableXctId* lock_;

  /**
   * Whick lock mode we should take according to RLL. We might take a stronger lock
   * than this
   */
  LockMode preferred_mode_;

  /** Whick lock mode we have taken during the current run (of course initially kNoLock) */
  LockMode taken_mode_;

  /**
   * 0 means the lock not taken. mcs_block_ == 0 iff taken_mode_ == kNolock.
   */
  McsBlockIndex mcs_block_;

  // want to have these.. but maitaining them is nasty after sort. let's revisit later
  // ReadXctAccess* read_set_;
  // WriteXctAccess* write_set_;

  void set(
    UniversalLockId id,
    RwLockableXctId* lock,
    LockMode preferred_mode,
    LockMode taken_mode) {
    universal_lock_id_ = id;
    lock_ = lock;
    preferred_mode_ = preferred_mode;
    taken_mode_ = taken_mode;
    mcs_block_ = 0;
  }

  bool is_locked() const { return taken_mode_ != kNoLock; }
  bool is_enough() const { return taken_mode_ >= preferred_mode_; }

  bool operator<(const LockEntry& rhs) const {
    return universal_lock_id_ < rhs.universal_lock_id_;
  }

  friend std::ostream& operator<<(std::ostream& o, const LockEntry& v);

  /** for std::binary_search() etc without creating the object */
  struct LessThan {
    bool operator()(UniversalLockId lhs, const LockEntry& rhs) const {
      return lhs < rhs.universal_lock_id_;
    }
    bool operator()(const LockEntry& lhs, UniversalLockId rhs) const {
      return lhs.universal_lock_id_ < rhs;
    }
  };
};

/**
 * @brief Retrospective lock list.
 * @ingroup RLL
 * @details
 * This is \e NOT a POD because we need dynamic memory for the list.
 * @note This object itself is thread-private. No concurrency control needed.
 */
class RetrospectiveLockList {
 public:
  RetrospectiveLockList();
  ~RetrospectiveLockList();

  void init(
    LockEntry* array,
    uint32_t capacity,
    const memory::GlobalVolatilePageResolver& resolver);

  void uninit();
  void clear_entries();

  /**
   * Analogous to std::binary_search() for the given lock.
   * @return Index of an entry whose lock_ == lock. kLockListPositionInvalid if not found.
   */
  LockListPosition binary_search(UniversalLockId lock) const;

  /**
   * Analogous to std::lower_bound() for the given lock.
   * @return Index of the fist entry whose lock_ is not less than lock.
   * If no such entry, last_active_entry_ + 1U, whether last_active_entry_ is 0 or not.
   * Thus the return value is always >0. This is to immitate std::lower_bound's behavior.
   */
  LockListPosition lower_bound(UniversalLockId lock) const;

  /**
   * @brief Fill out this retrospetive lock list for the next run of the given transaction.
   * @param[in] context the thread conveying the transaction. must be currently running a xct
   * @param[in] read_lock_threshold we "recommend" a read lock in RLL for records whose page
   * have this value or more in the temperature-stat. This value should be a bit lower than
   * the threshold we trigger read-locks without RLL. Otherwise, the next run might often
   * take a read-lock the RLL discarded due to a concurrent abort, which might violate canonical
   * order.
   * @details
   * This method is invoked when a transaction aborts at precommit due to some conflict.
   * Based on the current transaction's read/write-sets, this builds an RLL
   * that contains locks we _probably_ should take next time in a canonical order.
   * @note we should keep an eye on the CPU cost of this method. Hopefully negligible.
   * We can speed up this method by merging RLL/CLL to read/write-set in xct so
   * that we don't need another sorting.
   */
  void construct(thread::Thread* context, uint32_t read_lock_threshold);

  const LockEntry* get_array() const { return array_; }
  LockEntry* get_entry(LockListPosition pos) {
    ASSERT_ND(is_valid_entry(pos));
    return array_ + pos;
  }
  const LockEntry* get_entry(LockListPosition pos) const {
    ASSERT_ND(is_valid_entry(pos));
    return array_ + pos;
  }
  uint32_t get_capacity() const { return capacity_; }
  LockListPosition get_last_active_entry() const { return last_active_entry_; }
  bool is_valid_entry(LockListPosition pos) const {
    return pos != kLockListPositionInvalid && pos <= last_active_entry_;
  }
  bool is_empty() const { return last_active_entry_ == kLockListPositionInvalid; }

  friend std::ostream& operator<<(std::ostream& o, const RetrospectiveLockList& v);
  void assert_sorted() const ALWAYS_INLINE;
  void assert_sorted_impl() const;

  LockEntry* begin() { return array_ + 1U; }
  LockEntry* end() { return array_ + 1U + last_active_entry_; }
  const LockEntry* cbegin() const { return array_ + 1U; }
  const LockEntry* cend() const { return array_ + 1U + last_active_entry_; }
  const memory::GlobalVolatilePageResolver& get_volatile_page_resolver() const {
    return volatile_page_resolver_;
  }

 private:
  /**
   * Array of retrospective lock entries in the previous run.
   * Index-0 is reserved as a dummy entry, so array_[1] and onwards are used.
   * Entries are distinct, so no two entries have same lock_ value.
   * For example, when the previous run took read and write locks on the same lock,
   * we merge it to one entry of write-lock.
   */
  LockEntry* array_;
  /**
   * Volatile page resolver for converting between lock ptr and UniversalLockId.
   */
  memory::GlobalVolatilePageResolver volatile_page_resolver_;
  /**
   * Capacity of the array_. Max count of LockEntry, \e NOT bytes.
   * Note that index-0 is a dummy entry, so capacity_ must be #-of-active-entries + 1.
   * In most cases, capacity is much larger than # of locks in one xct, tho...
   */
  uint32_t capacity_;
  /**
   * Index of the last active entry in the RLL.
   * kLockListPositionInvalid if this list is empty.
   */
  LockListPosition last_active_entry_;

  LockListPosition issue_new_position() {
    ++last_active_entry_;
    ASSERT_ND(last_active_entry_ < capacity_);
    return last_active_entry_;
  }
};

/**
 * @brief Sorted list of all locks, either read-lock or write-lock, taken in the current run.
 * @ingroup RLL
 * @details
 * This is \e NOT a POD because we need dynamic memory for the list.
 * This holds all locks in address order to help our commit protocol.
 *
 * We so far maintain this object in addition to read-set/write-set.
 * We can merge these objects in to one, which will allow us to add some functionality
 * and optimizations. For example, "read my own write" semantics would be
 * practically implemented with such an integrated list.
 * We can reduce the number of sorting and tracking, too.
 * But, let's do them later.
 * @note This object itself is thread-private. No concurrency control needed.
 */
class CurrentLockList {
 public:
  CurrentLockList();
  ~CurrentLockList();

  void init(
    LockEntry* array,
    uint32_t capacity,
    const memory::GlobalVolatilePageResolver& resolver);
  void uninit();
  void clear_entries();

  /**
   * Analogous to std::binary_search() for the given lock.
   * @return Index of an entry whose lock_ == lock. kLockListPositionInvalid if not found.
   */
  LockListPosition binary_search(UniversalLockId lock) const;

  /**
   * Adds an entry to this list, re-sorting part of the list if necessary to keep the sortedness.
   * If there is an existing entry for the lock, it just returns its position.
   * If not, this method creates a new entry with taken_mode=kNoLock.
   * @return the position of the newly added entry. kLockListPositionInvalid means the list
   * was full and couldn't add (which is very unlikely, tho).
   * @note Do not frequently use this method. You should batch your insert when you can.
   * This method is used when we couldn't batch/expect the new entry.
   * @see batch_insert_write_placeholders()
   */
  LockListPosition get_or_add_entry(
    UniversalLockId lock_id,
    RwLockableXctId* lock,
    LockMode preferred_mode);

  /**
   * Analogous to std::lower_bound() for the given lock.
   * @return Index of the fist entry whose lock_ is not less than lock.
   * If no such entry, last_active_entry_ + 1U, whether last_active_entry_ is 0 or not.
   * Thus the return value is always >0. This is to immitate std::lower_bound's behavior.
   */
  LockListPosition lower_bound(UniversalLockId lock) const;

  const LockEntry* get_array() const { return array_; }
  LockEntry* get_array() { return array_; }
  LockEntry* get_entry(LockListPosition pos) {
    ASSERT_ND(is_valid_entry(pos));
    return array_ + pos;
  }
  const LockEntry* get_entry(LockListPosition pos) const {
    ASSERT_ND(is_valid_entry(pos));
    return array_ + pos;
  }
  uint32_t get_capacity() const { return capacity_; }
  LockListPosition get_last_active_entry() const { return last_active_entry_; }
  bool is_valid_entry(LockListPosition pos) const {
    return pos != kLockListPositionInvalid && pos <= last_active_entry_;
  }
  bool is_empty() const { return last_active_entry_ == kLockListPositionInvalid; }

  friend std::ostream& operator<<(std::ostream& o, const CurrentLockList& v);
  void assert_sorted() const ALWAYS_INLINE;
  void assert_sorted_impl() const;

  LockEntry* begin() { return array_ + 1U; }
  LockEntry* end() { return array_ + 1U + last_active_entry_; }
  const LockEntry* cbegin() const { return array_ + 1U; }
  const LockEntry* cend() const { return array_ + 1U + last_active_entry_; }
  const memory::GlobalVolatilePageResolver& get_volatile_page_resolver() const {
    return volatile_page_resolver_;
  }

  /**
   * @returns largest index of entries that are already locked.
   * kLockListPositionInvalid if no entry is locked.
   */
  LockListPosition get_last_locked_entry() const { return last_locked_entry_; }
  /** @returns the biggest Id of locks actually locked. If none, kNullUniversalLockId. */
  UniversalLockId get_max_locked_id() const {
    if (last_locked_entry_ == kLockListPositionInvalid) {
      return kNullUniversalLockId;
    } else {
      return get_entry(last_locked_entry_)->universal_lock_id_;
    }
  }
  /** Calculate last_locked_entry_ by really checking the whole list. */
  LockListPosition calculate_last_locked_entry() const {
    return calculate_last_locked_entry_from(last_active_entry_);
  }
  /** Only searches among entries at or before "from" */
  LockListPosition calculate_last_locked_entry_from(LockListPosition from) const {
    for (LockListPosition pos = from; pos > kLockListPositionInvalid; --pos) {
      if (array_[pos].is_locked()) {
        return pos;
      }
    }
    return kLockListPositionInvalid;
  }
  void assert_last_locked_entry() const {
#ifndef NDEBUG
    LockListPosition correct = calculate_last_locked_entry();
    ASSERT_ND(correct == last_locked_entry_);
#endif  // NDEBUG
  }

  /**
   * @brief Create entries for all write-sets in one-shot.
   * @param[in] write_set write-sets to create placeholders for. Must be canonically sorted.
   * @param[in] write_set_size count of entries in write_set
   * @details
   * During precommit, we must create an entry for every write-set.
   * Rather than doing it one by one, this method creates placeholder entries for all of them.
   * The placeholders are not locked yet (taken_mode_ == kNoLock).
   */
  void batch_insert_write_placeholders(const WriteXctAccess* write_set, uint32_t write_set_size);

  /**
   * Another batch-insert method used at the beginning of a transaction.
   * When an Xct has RLL, it will highly likely lock all of them.
   * So, it pre-populates CLL entries for all of them at the beginning.
   * This is both for simplicity and performance.
   * @pre is_empty() : call this at the beginning of xct.
   * @pre !rll.is_empty() : then why the heck are you calling.
   */
  void prepopulate_for_retrospective_lock_list(const RetrospectiveLockList& rll);

  ////////////////////////////////////////////////////////////////////////
  ///
  /// Methods below take or release locks, so they receive MCS_RW_IMPL, a template param.
  /// To avoid vtable and allow inlining, we define them at the bottom of this file.
  ///
  ////////////////////////////////////////////////////////////////////////

  /**
   * @brief Acquire one lock in this CLL.
   * @details
   * This method automatically checks if we are following canonical mode,
   * and acquire the lock unconditionally when in canonical mode (never returns until acquire),
   * and try the lock instanteneously when not in canonical mode (returns RaceAbort immediately).
   */
  template<typename MCS_RW_IMPL>
  ErrorCode try_or_acquire_single_lock(LockListPosition pos, MCS_RW_IMPL* mcs_rw_impl);

  /**
   * @brief Acquire multiple locks up to the given position in canonical order.
   * @details
   * This is invoked by the thread to keep itself in canonical mode.
   * This method is \e unconditional, meaning waits forever until we acquire the locks.
   * Hence, this method must be invoked when the thread is still in canonical mode.
   * Otherwise, it risks deadlock.
   */
  template<typename MCS_RW_IMPL>
  ErrorCode try_or_acquire_multiple_locks(LockListPosition upto_pos, MCS_RW_IMPL* mcs_rw_impl);

  template<typename MCS_RW_IMPL>
  void try_async_single_lock(LockListPosition pos, MCS_RW_IMPL* mcs_rw_impl);
  template<typename MCS_RW_IMPL>
  bool retry_async_single_lock(LockListPosition pos, MCS_RW_IMPL* mcs_rw_impl);
  template<typename MCS_RW_IMPL>
  void cancel_async_single_lock(LockListPosition pos, MCS_RW_IMPL* mcs_rw_impl);

  template<typename MCS_RW_IMPL>
  void try_async_multiple_locks(LockListPosition upto_pos, MCS_RW_IMPL* mcs_rw_impl);

  /** * Release all locks in CLL */
  template<typename MCS_RW_IMPL>
  void        release_all_locks(MCS_RW_IMPL* mcs_rw_impl) {
    release_all_after(kNullUniversalLockId, mcs_rw_impl);
  }
  /**
   * Release all locks in CLL whose addresses are canonically ordered
   * before the parameter. This is used where we need to rule out the risk of deadlock.
   * Unlike clear_entries(), this leaves the entries.
   */
  template<typename MCS_RW_IMPL>
  void        release_all_after(UniversalLockId address, MCS_RW_IMPL* mcs_rw_impl);
  /** same as release_all_after(address - 1) */
  template<typename MCS_RW_IMPL>
  void        release_all_at_and_after(UniversalLockId address, MCS_RW_IMPL* mcs_rw_impl);
  /**
   * This \e gives-up locks in CLL that are not yet taken.
   * preferred mode will be set to either NoLock or same as taken_mode,
   * and all incomplete async locks will be cancelled.
   * Unlike clear_entries(), this leaves the entries.
   */
  template<typename MCS_RW_IMPL>
  void        giveup_all_after(UniversalLockId address, MCS_RW_IMPL* mcs_rw_impl);
  template<typename MCS_RW_IMPL>
  void        giveup_all_at_and_after(UniversalLockId address, MCS_RW_IMPL* mcs_rw_impl);

 private:
  /**
   * Array of lock entries in the current run.
   * Index-0 is reserved as a dummy entry, so array_[1] and onwards are used.
   * Entries are distinct.
   */
  LockEntry* array_;
  /**
   * Volatile page resolver for converting between lock ptr and UniversalLockId.
   */
  memory::GlobalVolatilePageResolver volatile_page_resolver_;
  /**
   * Capacity of the array_. Max count of RetrospectiveLock, \e NOT bytes.
   * Note that index-0 is a dummy entry, so capacity_ must be #-of-active-entries + 1.
   * In most cases, capacity is much larger than # of locks in one xct, tho...
   */
  uint32_t capacity_;
  /**
   * Index of the last active entry in the CLL.
   * kLockListPositionInvalid if this list is empty.
   */
  LockListPosition last_active_entry_;
  /**
   * largest index of entries that are already locked.
   * kLockListPositionInvalid if no entry is locked.
   */
  LockListPosition last_locked_entry_;

  LockListPosition issue_new_position() {
    assert_last_locked_entry();
    ++last_active_entry_;
    ASSERT_ND(last_active_entry_ < capacity_);
    return last_active_entry_;
  }

  void release_all_after_debuglog(
    uint32_t released_read_locks,
    uint32_t released_write_locks,
    uint32_t already_released_locks,
    uint32_t canceled_async_read_locks,
    uint32_t canceled_async_write_locks) const;

  void giveup_all_after_debuglog(
    uint32_t givenup_read_locks,
    uint32_t givenup_write_locks,
    uint32_t givenup_upgrades,
    uint32_t already_enough_locks,
    uint32_t canceled_async_read_locks,
    uint32_t canceled_async_write_locks) const;
};

inline void RetrospectiveLockList::assert_sorted() const {
  // In release mode, this code must be completely erased by compiler
#ifndef  NDEBUG
  assert_sorted_impl();
#endif  // NDEBUG
}

inline void CurrentLockList::assert_sorted() const {
#ifndef  NDEBUG
  assert_sorted_impl();
#endif  // NDEBUG
}

////////////////////////////////////////////////////////////////////////
/// Inline definitions of CurrentLockList methods below.
/// These are inlined primarily because they receive a template param,
/// not because we want to inline for performance.
/// We could do explicit instantiations, but not that lengthy, either.
/// Just inlining them is easier in this case.
////////////////////////////////////////////////////////////////////////
template<typename MCS_RW_IMPL>
inline ErrorCode CurrentLockList::try_or_acquire_single_lock(
  LockListPosition pos,
  MCS_RW_IMPL* mcs_rw_impl) {
  LockEntry* lock_entry = get_entry(pos);
  if (lock_entry->is_enough()) {
    return kErrorCodeOk;
  }
  ASSERT_ND(lock_entry->taken_mode_ != kWriteLock);

  McsRwLock* lock_addr = lock_entry->lock_->get_key_lock();
  if (lock_entry->taken_mode_ != kNoLock) {
    ASSERT_ND(lock_entry->preferred_mode_ == kWriteLock);
    ASSERT_ND(lock_entry->taken_mode_ == kReadLock);
    ASSERT_ND(lock_entry->mcs_block_);
    // This is reader->writer upgrade.
    // We simply release read-lock first then take write-lock in this case.
    // In traditional 2PL, such an unlock-then-lock violates serializability,
    // but we guarantee serializability by read-verification anyways.
    // We can release any lock anytime.. great flexibility!
    mcs_rw_impl->release_rw_reader(lock_addr, lock_entry->mcs_block_);
    lock_entry->taken_mode_ = kNoLock;
    lock_entry->mcs_block_ = 0;
    // Calculate last_locked_entry_ by scanning the whole list - during upgrade
    // a reader-lock might get released and re-acquired in writer mode, violating
    // canonical mode. In these cases last_locked_entry_ should not change unless
    // the lock being upgraded is indeed the last entry.
    last_locked_entry_ = calculate_last_locked_entry();
  } else {
    // This method is for unconditional acquire and try, not aync/retry.
    // If we have a queue node already, something was misused.
    ASSERT_ND(lock_entry->mcs_block_ == 0);
  }

  // Now we need to take the lock. Are we in canonical mode?
  if (last_locked_entry_ == kLockListPositionInvalid || last_locked_entry_ < pos) {
    // yay, we are in canonical mode. we can unconditionally get the lock
    ASSERT_ND(lock_entry->taken_mode_ == kNoLock);  // not a lock upgrade, either
    if (lock_entry->preferred_mode_ == kWriteLock) {
      lock_entry->mcs_block_ = mcs_rw_impl->acquire_unconditional_rw_writer(lock_addr);
    } else {
      ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
      lock_entry->mcs_block_ = mcs_rw_impl->acquire_unconditional_rw_reader(lock_addr);
    }
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
    last_locked_entry_ = pos;
  } else {
    // hmm, we violated canonical mode. has a risk of deadlock.
    // Let's just try acquire the lock and immediately give up if it fails.
    // The RLL will take care of the next run.
    // TODO(Hideaki) release some of the lock we have taken to restore canonical mode.
    // We haven't imlpemented this optimization yet.
    ASSERT_ND(lock_entry->mcs_block_ == 0);
    ASSERT_ND(lock_entry->taken_mode_ == kNoLock);
    if (lock_entry->preferred_mode_ == kWriteLock) {
      lock_entry->mcs_block_ = mcs_rw_impl->acquire_try_rw_writer(lock_addr);
    } else {
      ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
      lock_entry->mcs_block_ = mcs_rw_impl->acquire_try_rw_reader(lock_addr);
    }
    if (lock_entry->mcs_block_ == 0) {
      return kErrorCodeXctLockAbort;
    }
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
  }
  ASSERT_ND(lock_entry->mcs_block_);
  ASSERT_ND(lock_entry->is_locked());
  assert_last_locked_entry();
  return kErrorCodeOk;
}

template<typename MCS_RW_IMPL>
inline ErrorCode CurrentLockList::try_or_acquire_multiple_locks(
  LockListPosition upto_pos,
  MCS_RW_IMPL* mcs_rw_impl) {
  ASSERT_ND(upto_pos != kLockListPositionInvalid);
  ASSERT_ND(upto_pos <= last_active_entry_);
  // Especially in this case, we probably should release locks after upto_pos first.
  for (LockListPosition pos = 1U; pos <= upto_pos; ++pos) {
    CHECK_ERROR_CODE(try_or_acquire_single_lock(pos, mcs_rw_impl));
  }
  return kErrorCodeOk;
}

template<typename MCS_RW_IMPL>
inline void CurrentLockList::try_async_single_lock(
  LockListPosition pos,
  MCS_RW_IMPL* mcs_rw_impl) {
  LockEntry* lock_entry = get_entry(pos);
  if (lock_entry->is_enough()) {
    return;
  }
  ASSERT_ND(lock_entry->taken_mode_ != kWriteLock);

  McsRwLock* lock_addr = lock_entry->lock_->get_key_lock();
  if (lock_entry->taken_mode_ != kNoLock) {
    ASSERT_ND(lock_entry->preferred_mode_ == kWriteLock);
    ASSERT_ND(lock_entry->taken_mode_ == kReadLock);
    ASSERT_ND(lock_entry->mcs_block_);
    // This is reader->writer upgrade.
    // We simply release read-lock first then take write-lock in this case.
    // In traditional 2PL, such an unlock-then-lock violates serializability,
    // but we guarantee serializability by read-verification anyways.
    // We can release any lock anytime.. great flexibility!
    mcs_rw_impl->release_rw_reader(lock_addr, lock_entry->mcs_block_);
    lock_entry->taken_mode_ = kNoLock;
    last_locked_entry_ = calculate_last_locked_entry_from(pos - 1U);
    assert_last_locked_entry();
  } else {
    // This function is for pushing the queue node in the extended rwlock.
    // Doomed if we already have a queue node.
    ASSERT_ND(lock_entry->mcs_block_ == 0);
  }

  // Don't really care canonical order here, just send out the request.
  AcquireAsyncRet async_ret;
  if (lock_entry->preferred_mode_ == kWriteLock) {
    async_ret = mcs_rw_impl->acquire_async_rw_writer(lock_addr);
  } else {
    ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
    async_ret = mcs_rw_impl->acquire_async_rw_reader(lock_addr);
  }
  ASSERT_ND(async_ret.block_index_);
  lock_entry->mcs_block_ = async_ret.block_index_;
  if (async_ret.acquired_) {
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
    ASSERT_ND(lock_entry->is_enough());
  }
  ASSERT_ND(lock_entry->mcs_block_);
}

template<typename MCS_RW_IMPL>
inline bool CurrentLockList::retry_async_single_lock(
  LockListPosition pos,
  MCS_RW_IMPL* mcs_rw_impl) {
  LockEntry* lock_entry = get_entry(pos);
  // Must be not taken yet, and must have pushed a qnode to the lock queue
  ASSERT_ND(!lock_entry->is_enough());
  ASSERT_ND(lock_entry->taken_mode_ == kNoLock);
  ASSERT_ND(lock_entry->mcs_block_);
  ASSERT_ND(!lock_entry->is_locked());

  McsRwLock* lock_addr = lock_entry->lock_->get_key_lock();
  bool acquired = false;
  if (lock_entry->preferred_mode_ == kWriteLock) {
    acquired = mcs_rw_impl->retry_async_rw_writer(lock_addr, lock_entry->mcs_block_);
  } else {
    ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
    acquired = mcs_rw_impl->retry_async_rw_reader(lock_addr, lock_entry->mcs_block_);
  }
  if (acquired) {
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
    ASSERT_ND(lock_entry->is_locked());
    last_locked_entry_ = std::max(last_locked_entry_, pos);
    assert_last_locked_entry();
  }
  return acquired;
}

template<typename MCS_RW_IMPL>
inline void CurrentLockList::cancel_async_single_lock(
  LockListPosition pos,
  MCS_RW_IMPL* mcs_rw_impl) {
  LockEntry* lock_entry = get_entry(pos);
  ASSERT_ND(!lock_entry->is_enough());
  ASSERT_ND(lock_entry->taken_mode_ == kNoLock);
  ASSERT_ND(lock_entry->mcs_block_);
  McsRwLock* lock_addr = lock_entry->lock_->get_key_lock();
  if (lock_entry->preferred_mode_ == kReadLock) {
    mcs_rw_impl->cancel_async_rw_reader(lock_addr, lock_entry->mcs_block_);
  } else {
    ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
    mcs_rw_impl->cancel_async_rw_writer(lock_addr, lock_entry->mcs_block_);
  }
  lock_entry->mcs_block_ = 0;
}

template<typename MCS_RW_IMPL>
inline void CurrentLockList::try_async_multiple_locks(
  LockListPosition upto_pos,
  MCS_RW_IMPL* mcs_rw_impl) {
  ASSERT_ND(upto_pos != kLockListPositionInvalid);
  ASSERT_ND(upto_pos <= last_active_entry_);
  for (LockListPosition pos = 1U; pos <= upto_pos; ++pos) {
    try_async_single_lock(pos, mcs_rw_impl);
  }
}

template<typename MCS_RW_IMPL>
inline void CurrentLockList::release_all_after(UniversalLockId address, MCS_RW_IMPL* mcs_rw_impl) {
  // Only this and below logics are implemented here because this needs to know about CLL.
  // This is not quite about the lock algorithm itself. It's something on higher level.
  assert_sorted();
  uint32_t released_read_locks = 0;
  uint32_t released_write_locks = 0;
  uint32_t already_released_locks = 0;
  uint32_t canceled_async_read_locks = 0;
  uint32_t canceled_async_write_locks = 0;

  LockListPosition new_last_locked_entry = kLockListPositionInvalid;
  for (LockEntry* entry = begin(); entry != end(); ++entry) {
    if (entry->universal_lock_id_ <= address) {
      if (entry->is_locked()) {
        new_last_locked_entry = entry - array_;
      }
      continue;
    }
    if (entry->is_locked()) {
      if (entry->taken_mode_ == kReadLock) {
        mcs_rw_impl->release_rw_reader(entry->lock_->get_key_lock(), entry->mcs_block_);
        ++released_read_locks;
      } else {
        ASSERT_ND(entry->taken_mode_ == kWriteLock);
        mcs_rw_impl->release_rw_writer(entry->lock_->get_key_lock(), entry->mcs_block_);
        ++released_write_locks;
      }
      entry->mcs_block_ = 0;
      entry->taken_mode_ = kNoLock;
    } else if (entry->mcs_block_) {
      // Not locked yet, but we have mcs_block_ set, this means we tried it in
      // async mode, then still waiting or at least haven't confirmed that we acquired it.
      // Cancel these "retrieable" locks to which we already pushed our qnode.
      ASSERT_ND(entry->taken_mode_ == kNoLock);
      if (entry->preferred_mode_ == kReadLock) {
        mcs_rw_impl->cancel_async_rw_reader(entry->lock_->get_key_lock(), entry->mcs_block_);
        ++canceled_async_read_locks;
      } else {
        ASSERT_ND(entry->preferred_mode_ == kWriteLock);
        mcs_rw_impl->cancel_async_rw_writer(entry->lock_->get_key_lock(), entry->mcs_block_);
        ++canceled_async_write_locks;
      }
      entry->mcs_block_ = 0;
    } else {
      ASSERT_ND(entry->taken_mode_ == kNoLock);
      ++already_released_locks;
    }
  }

  last_locked_entry_ = new_last_locked_entry;
  assert_last_locked_entry();

#ifndef NDEBUG
  release_all_after_debuglog(
    released_read_locks,
    released_write_locks,
    already_released_locks,
    canceled_async_read_locks,
    canceled_async_write_locks);
#endif  // NDEBUG
}
template<typename MCS_RW_IMPL>
inline void CurrentLockList::release_all_at_and_after(
  UniversalLockId address,
  MCS_RW_IMPL* mcs_rw_impl) {
  if (address == kNullUniversalLockId) {
    release_all_after<MCS_RW_IMPL>(kNullUniversalLockId, mcs_rw_impl);
  } else {
    release_all_after<MCS_RW_IMPL>(address - 1U, mcs_rw_impl);
  }
}

template<typename MCS_RW_IMPL>
inline void CurrentLockList::giveup_all_after(UniversalLockId address, MCS_RW_IMPL* mcs_rw_impl) {
  assert_sorted();
  uint32_t givenup_read_locks = 0;
  uint32_t givenup_write_locks = 0;
  uint32_t givenup_upgrades = 0;
  uint32_t already_enough_locks = 0;
  uint32_t canceled_async_read_locks = 0;
  uint32_t canceled_async_write_locks = 0;

  for (LockEntry* entry = begin(); entry != end(); ++entry) {
    if (entry->universal_lock_id_ <= address) {
      continue;
    }
    if (entry->preferred_mode_ == kNoLock) {
      continue;
    }
    if (entry->is_enough()) {
      ++already_enough_locks;
      continue;
    }

    if (entry->is_locked()) {
      ASSERT_ND(entry->taken_mode_ == kReadLock);
      ASSERT_ND(entry->preferred_mode_ == kWriteLock);
      ++givenup_upgrades;
      entry->preferred_mode_ = entry->taken_mode_;
    } else if (entry->mcs_block_) {
      if (entry->preferred_mode_ == kReadLock) {
        mcs_rw_impl->cancel_async_rw_reader(entry->lock_->get_key_lock(), entry->mcs_block_);
        ++canceled_async_read_locks;
      } else {
        ASSERT_ND(entry->preferred_mode_ == kWriteLock);
        mcs_rw_impl->cancel_async_rw_writer(entry->lock_->get_key_lock(), entry->mcs_block_);
        ++canceled_async_write_locks;
      }
      entry->mcs_block_ = 0;
      entry->preferred_mode_ = kNoLock;
    } else {
      ASSERT_ND(entry->taken_mode_ == kNoLock);
      if (entry->preferred_mode_ == kReadLock) {
        ++givenup_read_locks;
      } else {
        ASSERT_ND(entry->preferred_mode_ == kWriteLock);
        ++givenup_write_locks;
      }
      entry->preferred_mode_ = kNoLock;
    }
  }

#ifndef NDEBUG
  giveup_all_after_debuglog(
    givenup_read_locks,
    givenup_write_locks,
    givenup_upgrades,
    already_enough_locks,
    canceled_async_read_locks,
    canceled_async_write_locks);
#endif  // NDEBUG
}
template<typename MCS_RW_IMPL>
inline void CurrentLockList::giveup_all_at_and_after(
  UniversalLockId address,
  MCS_RW_IMPL* mcs_rw_impl) {
  if (address == kNullUniversalLockId) {
    giveup_all_after<MCS_RW_IMPL>(kNullUniversalLockId, mcs_rw_impl);
  } else {
    giveup_all_after<MCS_RW_IMPL>(address - 1U, mcs_rw_impl);
  }
}

////////////////////////////////////////////////////////////
/// General lower_bound/binary_search logic for any kind of LockList/LockEntry.
/// Used from retrospective_lock_list.cpp and sysxct_impl.cpp.
/// These implementations are skewed towards sorted cases,
/// meaning it runs faster when accesses are nicely sorted.
////////////////////////////////////////////////////////////
template<typename LOCK_LIST, typename LOCK_ENTRY>
inline LockListPosition lock_lower_bound(
  const LOCK_LIST& list,
  UniversalLockId lock) {
  LockListPosition last_active_entry = list.get_last_active_entry();
  if (last_active_entry == kLockListPositionInvalid) {
    return kLockListPositionInvalid + 1U;
  }
  // Check the easy cases first. This will be an wasted cost if it's not, but still cheap.
  const LOCK_ENTRY* array = list.get_array();
  // For example, [dummy, 3, 5, 7] (last_active_entry=3).
  // id=7: 3, larger: 4, smaller: need to check more
  if (array[last_active_entry].universal_lock_id_ == lock) {
    return last_active_entry;
  } else if (array[last_active_entry].universal_lock_id_ < lock) {
    return last_active_entry + 1U;
  }

  LockListPosition pos
    = std::lower_bound(
        array + 1U,
        array + last_active_entry + 1U,
        lock,
        typename LOCK_ENTRY::LessThan())
      - array;
  // in the above example, id=6: 3, id=4,5: 2, smaller: 1
  ASSERT_ND(pos != kLockListPositionInvalid);
  ASSERT_ND(pos <= last_active_entry);  // otherwise we went into the branch above
  ASSERT_ND(array[pos].universal_lock_id_ >= lock);
  ASSERT_ND(pos == 1U || array[pos - 1U].universal_lock_id_ < lock);
  return pos;
}

template<typename LOCK_LIST, typename LOCK_ENTRY>
inline LockListPosition lock_binary_search(
  const LOCK_LIST& list,
  UniversalLockId lock) {
  LockListPosition last_active_entry = list.get_last_active_entry();
  LockListPosition pos = lock_lower_bound<LOCK_LIST, LOCK_ENTRY>(list, lock);
  if (pos != kLockListPositionInvalid && pos <= last_active_entry) {
    const LOCK_ENTRY* array = list.get_array();
    if (array[pos].universal_lock_id_ == lock) {
      return pos;
    }
  }
  return kLockListPositionInvalid;
}


}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_RETROSPECTIVE_LOCK_LIST_HPP_
