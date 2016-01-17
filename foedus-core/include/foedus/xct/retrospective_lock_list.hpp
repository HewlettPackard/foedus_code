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

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief \b Retrospective \b Lock \b List (\b RLL) to avoid deadlocks.
 * @defgroup RLL
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
 * @brief An entry in RLL, representing one lock to take.
 * @ingroup RLL
 * @details
 * This is a POD, and guaranteed to be init-ed/reset-ed by memzero and copied via memcpy.
 */
struct RetrospectiveLock {
  /**
   * Used to order locks in canonical order.
   * So far universal_lock_id_ == reinterpret_cast<uintptr_t>(lock_).
   */
  UniversalLockId universal_lock_id_;

  /**
   * Virtual address of the lock.
   */
  RwLockableXctId* lock_;

  /** Whick lock mode we should take */
  LockMode preferred_mode_;

  /** Whick lock mode we have taken during the current run (of course initially kNoLock) */
  LockMode taken_mode_;

  char     pad_[8U - sizeof(preferred_mode_) - sizeof(taken_mode_)];

  void set(
    UniversalLockId id,
    RwLockableXctId* lock,
    LockMode preferred_mode,
    LockMode taken_mode) {
    universal_lock_id_ = id;
    lock_ = lock;
    preferred_mode_ = preferred_mode;
    taken_mode_ = taken_mode;
  }

  bool operator<(const RetrospectiveLock& rhs) const {
    return universal_lock_id_ < rhs.universal_lock_id_;
  }

  friend std::ostream& operator<<(std::ostream& o, const RetrospectiveLock& v);
};

/** for std::binary_search() etc without creating the object */
struct RetrospectiveLockLessThan {
  bool operator()(UniversalLockId lhs, const RetrospectiveLock& rhs) const {
    return lhs < rhs.universal_lock_id_;
  }
  bool operator()(const RetrospectiveLock& lhs, UniversalLockId rhs) const {
    return lhs.universal_lock_id_ < rhs;
  }
};

/**
 * @brief Sorted list of RetrospectiveLockList.
 * @ingroup RLL
 * @details
 * This is \e NOT a POD because we need dynamic memory for the list.
 * @note This object itself is thread-private. No concurrency control needed.
 */
class RetrospectiveLockList {
 public:
  typedef RetrospectiveLock EntryType;
  RetrospectiveLockList();
  ~RetrospectiveLockList();

  void init(RetrospectiveLock* array, uint32_t capacity);
  void uninit();
  void clear_entries();

  /**
   * Analogous to std::binary_search() for the given lock.
   * @return Index of an entry whose lock_ == lock. kLockListPositionInvalid if not found.
   */
  LockListPosition binary_search(RwLockableXctId* lock) const;

  /**
   * Analogous to std::lower_bound() for the given lock.
   * @return Index of the fist entry whose lock_ is not less than lock.
   * kLockListPositionInvalid if not such entry.
   */
  // LockListPosition lower_bound(RetrospectiveLock* lock) const; currently not needed

  /**
   * @brief Acquire retrospective locks before or at the given lock in canonical order.
   * @param[in] current_lock we retrospectively take locks before or at this lock
   * @param[in] current_lock_mode _if_ we also take current_lock in this method, the lock mode.
   * Ignored if this RLL doesn't contain current_lock.
   * Also, if this RLL contains current_lock as write-lock, and current_lock_mode is Read,
   * we ignore it and take write-lock right away. In other words, the retrospect overwrites
   * the current need.
   * @details
   * This is invoked by the thread to keep itself in canonical mode.
   * This method is \e unconditional, meaning waits forever until we acquire the locks.
   * Hence, this method must be invoked when the thread is still in canonical mode.
   * Otherwise, it risks deadlock.
   */
  void acquire_retrospective_locks(
    thread::Thread* context,
    RwLockableXctId* current_lock,
    LockMode current_lock_mode);

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

  const RetrospectiveLock* get_array() const { return array_; }
  uint32_t get_capacity() const { return capacity_; }
  LockListPosition get_last_active_entry() const { return last_active_entry_; }

  friend std::ostream& operator<<(std::ostream& o, const RetrospectiveLockList& v);
  void assert_sorted() const ALWAYS_INLINE;
  void assert_sorted_impl() const;

 private:
  /**
   * Array of retrospective lock entries in the previous run.
   * Index-0 is reserved as a dummy entry, so array_[1] and onwards are used.
   * Entries are distinct, so no two entries have same lock_ value.
   * For example, when the previous run took read and write locks on the same lock,
   * we merge it to one entry of write-lock.
   */
  RetrospectiveLock* array_;
  /**
   * Capacity of the array_. Max count of RetrospectiveLock, \e NOT bytes.
   * Note that index-0 is a dummy entry, so capacity_ must be #-of-active-entries + 1.
   * In most cases, capacity is much larger than # of locks in one xct, tho...
   */
  uint32_t capacity_;
  /**
   * Index of the last active entry in the RLL.
   * kLockListPositionInvalid if this list is empty.
   */
  LockListPosition last_active_entry_;
  /**
   * Index of the last entry we locked in the current run in canonical mode.
   * kLockListPositionInvalid if we haven't locked any in the current run.
   */
  LockListPosition last_canonically_locked_entry_;

  LockListPosition issue_new_position() {
    ++last_active_entry_;
    ASSERT_ND(last_active_entry_ < capacity_);
    return last_active_entry_;
  }
};


/**
 * @brief Represents a lock taken in the current run.
 * @ingroup RLL
 * @details
 * This is a POD, and guaranteed to be init-ed/reset-ed by memzero and copied via memcpy.
 */
struct CurrentLock {
  /**
   * Used to order locks in canonical order.
   * So far universal_lock_id_ == reinterpret_cast<uintptr_t>(lock_).
   */
  UniversalLockId universal_lock_id_;

  /**
   * Virtual address of the lock.
   */
  RwLockableXctId* lock_;

  /** Whick lock mode we have taken during the current run (of course initially kNoLock) */
  LockMode taken_mode_;

  char     pad_[8U - sizeof(taken_mode_)];

  // want to have these.. but maitaining them is nasty after sort. let's revisit later
  // ReadXctAccess* read_set_;
  // WriteXctAccess* write_set_;

  void set(UniversalLockId id, RwLockableXctId* lock, LockMode taken_mode) {
    universal_lock_id_ = id;
    lock_ = lock;
    taken_mode_ = taken_mode;
  }

  bool operator<(const CurrentLock& rhs) const {
    return universal_lock_id_ < rhs.universal_lock_id_;
  }

  friend std::ostream& operator<<(std::ostream& o, const CurrentLock& v);
};

/** for std::binary_search() etc without creating the object */
struct CurrentLockLessThan {
  bool operator()(UniversalLockId lhs, const CurrentLock& rhs) const {
    return lhs < rhs.universal_lock_id_;
  }
  bool operator()(const CurrentLock& lhs, UniversalLockId rhs) const {
    return lhs.universal_lock_id_ < rhs;
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
  typedef CurrentLock EntryType;
  CurrentLockList();
  ~CurrentLockList();

  void init(CurrentLock* array, uint32_t capacity);
  void uninit();
  void clear_entries();

  /**
   * Analogous to std::binary_search() for the given lock.
   * @return Index of an entry whose lock_ == lock. kLockListPositionInvalid if not found.
   */
  LockListPosition binary_search(RwLockableXctId* lock) const;

  /**
   * Adds an entry to this list, re-sorting part of the list if necessary to keep the sortedness.
   * @return the position of the newly added entry. kLockListPositionInvalid means the list
   * was full and couldn't add (which is very unlikely, tho).
   */
  LockListPosition add_entry(RwLockableXctId* lock, LockMode taken_mode);

  /**
   * Analogous to std::lower_bound() for the given lock.
   * @return Index of the fist entry whose lock_ is not less than lock.
   * kLockListPositionInvalid if not such entry.
   */
  // LockListPosition lower_bound(RetrospectiveLock* lock) const; currently not needed

  /**
   * @return whether we can acquire the given lock in canonical mode.
   */
  bool is_in_canonical_mode(const RwLockableXctId* lock) const {
    if (!in_canonical_mode_) {
      // We already violated the order. no hope.
      return false;
    }
    if (last_active_entry_ == kLockListPositionInvalid) {
      // we don't have any, so trivially canonical
      return true;
    }
    ASSERT_ND(last_active_entry_ < capacity_);

    // Did we take any lock that is ordered before the lock?
    UniversalLockId id = to_universal_lock_id(lock);
    return id >= array_[last_active_entry_].universal_lock_id_;
  }

  const CurrentLock* get_array() const { return array_; }
  uint32_t get_capacity() const { return capacity_; }
  LockListPosition get_last_active_entry() const { return last_active_entry_; }

  friend std::ostream& operator<<(std::ostream& o, const CurrentLockList& v);
  void assert_sorted() const ALWAYS_INLINE;
  void assert_sorted_impl() const;

 private:
  /**
   * Array of lock entries in the current run.
   * Index-0 is reserved as a dummy entry, so array_[1] and onwards are used.
   * Entries are distinct.
   */
  CurrentLock* array_;
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
   * Whether we are still in canonical lock mode.
   */
  bool in_canonical_mode_;

  LockListPosition issue_new_position() {
    ++last_active_entry_;
    ASSERT_ND(last_active_entry_ < capacity_);
    return last_active_entry_;
  }
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

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_RETROSPECTIVE_LOCK_LIST_HPP_
