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
#ifndef FOEDUS_XCT_SYSXCT_IMPL_HPP_
#define FOEDUS_XCT_SYSXCT_IMPL_HPP_

#include <algorithm>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/error_code.hpp"
#include "foedus/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/sysxct_functor.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_mcs_impl.hpp"

namespace foedus {
namespace xct {

/**
 * @defgroup SYSXCT Physical-only, short-living system transactions.
 * @ingroup XCT
 * @details
 * A system transaction is a special transaction that changes nothing logically.
 * It changes only physical structures. For example, it splits a page or expand
 * a physical record without changing their logical data to help normal, user transactions (Xct).
 *
 * There are several prior work in databases to use system tranasctions.
 * System transactions in FOEDUS differ a bit from others in the following sense:
 * \li No transaction logging at all. FOEDUS's logical-logging protocol needs no transaction
 * logs for physical-only operations. Sysxct has no interaction with log entries.
 * \li Sysxct might take \e both page and record locks. In prior work, system transactions
 * take no record locks.
 * \li Instead, normal transactions in FOEDUS never take page locks. It's always a system
 * transaction to lock a page. Either normal or system, reads don't take page locks.
 * \li Sysxct might abort. Because of the page and record locks, there is a chance of forming
 * wait-for cycles. If this happens or might happen, Sysxct aborts. Thanks to the no-logging
 * property, aborts cause no aftermath, though.
 *
 * System transactions are not exposed to users, so the whole file is _impl.
 */

// TASK(Hideaki) this comparator probably has a common use. move to somewhere.
inline UniversalLockId to_universal_lock_id(storage::VolatilePagePointer page_id, uintptr_t addr) {
  return to_universal_lock_id(page_id.get_numa_node(), page_id.get_offset(), addr);
}
struct PageComparator {
  bool operator()(storage::Page* p1, storage::Page* p2) const {
    auto id1 = to_universal_lock_id(p1->get_volatile_page_id(), reinterpret_cast<uintptr_t>(p1));
    auto id2 = to_universal_lock_id(p2->get_volatile_page_id(), reinterpret_cast<uintptr_t>(p2));
    return id1 < id2;
  }
};

/**
 * @brief An entry in CLL/RLL for system transactions.
 * @ingroup SYSXCT
 * @details
 * Analogous to LockEntry, but a bit different because system transactions also take page locks.
 */
struct SysxctLockEntry {
  /**
   * Used to order locks in canonical order.
   * So far universal_lock_id_ == reinterpret_cast<uintptr_t>(lock_).
   */
  UniversalLockId universal_lock_id_;

  /**
   * Virtual address of a record lock. Either this or the page_lock is non-null.
   * Either RwLockableXctId* or Page*
   */
  uintptr_t       lock_;

  // No "preferred"/"taken" mode because system transactions always take W-locks, record or page.

  /**
   * 0 means the lock not taken.
   */
  McsBlockIndex   mcs_block_;

  /** Whether this is a pge lock or not */
  bool            page_lock_;

  /**
   * whether the lock was requested at least once in this run. If false, we will
   * not include this lock in next run.
   */
  bool            used_in_this_run_;

  void clear() {
    universal_lock_id_ = 0;
    lock_ = kNullUniversalLockId;
    mcs_block_ = 0;
    page_lock_ = false;
    used_in_this_run_ = false;
  }

  void set(UniversalLockId lock_id, uintptr_t lock, bool page_lock) {
    universal_lock_id_ = lock_id;
    lock_ = lock;
    mcs_block_ = 0;
    page_lock_ = page_lock;
    used_in_this_run_ = true;
  }

  RwLockableXctId* get_as_record_lock() const {
    ASSERT_ND(!page_lock_);
    return reinterpret_cast<RwLockableXctId*>(lock_);
  }
  storage::Page* get_as_page_lock() const {
    ASSERT_ND(page_lock_);
    return reinterpret_cast<storage::Page*>(lock_);
  }

  bool is_locked() const { return mcs_block_ != 0; }

  bool operator<(const SysxctLockEntry& rhs) const {
    return universal_lock_id_ < rhs.universal_lock_id_;
  }

  friend std::ostream& operator<<(std::ostream& o, const SysxctLockEntry& v);

  /** for std::binary_search() etc without creating the object */
  struct LessThan {
    bool operator()(UniversalLockId lhs, const SysxctLockEntry& rhs) const {
      return lhs < rhs.universal_lock_id_;
    }
    bool operator()(const SysxctLockEntry& lhs, UniversalLockId rhs) const {
      return lhs.universal_lock_id_ < rhs;
    }
  };
};

/**
 * @brief RLL/CLL of a system transaction.
 * @details
 * Unlike the normal transaction, lock-list is simpler in sysxct.
 * Sysxct doesn't need read-set or asynchronous locking. It immediately takes
 * the locks required by the logic, and abort if it couldn't.
 * Unlike normal xct, sysxct also locks records for just a few times (counting "lock all records
 * in this page" as one).
 * We thus integrate RLL and CLL into this list in system transactions.
 * We might follow this approach in normal xct later.
 *
 * This object is a POD. We use a fixed-size array.
 */
class SysxctLockList {
 public:
  enum Constants {
    /**
     * Maximum number of locks one system transaction might take.
     * In most cases just a few. The worst case is page-split where
     * a sysxct takes a page-full of records.
     */
    kMaxSysxctLocks = 1 << 10,
  };

  SysxctLockList() { init(); }
  ~SysxctLockList() {}

  // No copy
  SysxctLockList(const SysxctLockList& other) = delete;
  SysxctLockList& operator=(const SysxctLockList& other) = delete;

  void init() {
    last_locked_entry_ = kLockListPositionInvalid;  // to avoid assertions
    last_active_entry_ = kLockListPositionInvalid;
    clear_entries(kNullUniversalLockId);
  }

  /**
   * Remove all entries. This is used when a sysxct starts a fresh new transaction, not retry.
   */
  void clear_entries(UniversalLockId enclosing_max_lock_id) {
    ASSERT_ND(last_locked_entry_ == kLockListPositionInvalid);
    assert_sorted();
    last_locked_entry_ = kLockListPositionInvalid;
    last_active_entry_ = kLockListPositionInvalid;
    enclosing_max_lock_id_ = enclosing_max_lock_id;
    array_[kLockListPositionInvalid].clear();
  }

  /**
   * Unlike clear_entries(), this is used when a sysxct is aborted and will be retried.
   * We 'inherit' some entries (although all of them must be released) to help guide next runs.
   */
  void compress_entries(UniversalLockId enclosing_max_lock_id);

  /**
   * Analogous to std::lower_bound() for the given lock.
   * @return Index of the fist entry whose lock_ is not less than lock.
   * If no such entry, last_active_entry_ + 1U, whether last_active_entry_ is 0 or not.
   * Thus the return value is always >0. This is to immitate std::lower_bound's behavior.
   */
  LockListPosition lower_bound(UniversalLockId lock) const;

  /**
   * If not yet acquired, acquires a record lock and adds an entry to this list,
   * re-sorting part of the list if necessary to keep the sortedness.
   * If there is an existing entry for the lock, it resuses the entry.
   * @post Either the lock is taken or returns an error
   */
  template<typename MCS_ADAPTOR>
  ErrorCode request_record_lock(
    MCS_ADAPTOR mcs_adaptor,
    storage::VolatilePagePointer page_id,
    RwLockableXctId* lock) {
    uintptr_t lock_addr = reinterpret_cast<uintptr_t>(lock);
    return request_lock_general(mcs_adaptor, page_id, lock_addr, false);
  }

  /** Used to acquire many locks in a page at once. This reduces sorting cost. */
  template<typename MCS_ADAPTOR>
  ErrorCode batch_request_record_locks(
    MCS_ADAPTOR mcs_adaptor,
    storage::VolatilePagePointer page_id,
    uint32_t lock_count,
    RwLockableXctId** locks) {
    uintptr_t* lock_addr = reinterpret_cast<uintptr_t*>(locks);
    return batch_request_locks_general(mcs_adaptor, page_id, lock_count, lock_addr, false);
  }

  /** Acquires a page lock */
  template<typename MCS_ADAPTOR>
  ErrorCode request_page_lock(MCS_ADAPTOR mcs_adaptor, storage::Page* page) {
    uintptr_t lock_addr = reinterpret_cast<uintptr_t>(page);
    return request_lock_general(mcs_adaptor, page->get_volatile_page_id(), lock_addr, true);
  }

  /**
   * The interface is same as the record version, but this one doesn't do much optimization.
   * We shouldn't frequently lock many pages in sysxct.
   * However, at least this one locks the pages in UniversalLockId order to avoid deadlocks.
   */
  template<typename MCS_ADAPTOR>
  ErrorCode batch_request_page_locks(
    MCS_ADAPTOR mcs_adaptor,
    uint32_t lock_count,
    storage::Page** pages) {
    std::sort(pages, pages + lock_count, PageComparator());
    for (uint32_t i = 0; i < lock_count; ++i) {
      CHECK_ERROR_CODE(request_page_lock(mcs_adaptor, pages[i]));
    }
    return kErrorCodeOk;
  }

  /**
   * Releases all locks that were acquired. Entries are left as they were.
   * You should then call clear_entries() or compress_entries().
   */
  template<typename MCS_ADAPTOR>
  void release_all_locks(MCS_ADAPTOR mcs_adaptor);

  /**
   * @return position of the entry that corresponds to the lock. either added or found.
   * Never returns kLockListPositionInvalid.
   */
  LockListPosition get_or_add_entry(
    storage::VolatilePagePointer page_id,
    uintptr_t lock_addr,
    bool page_lock);

  /**
   * Batched version of get_or_add_entry().
   * @return position of the first entry that corresponds to any of the locks, either added or
   * found. Returns kLockListPositionInvalid iff lock_count == 0.
   * @note lock_addr does NOT have to be sorted. This function sorts it first (that's why
   * it's not const param). However, you SHOULD give this method a sorted array if it's easy to do.
   * This method first checks std::is_sorted() to skip sorting.
   */
  LockListPosition batch_get_or_add_entries(
    storage::VolatilePagePointer page_id,
    uint32_t lock_count,
    uintptr_t* lock_addr,
    bool page_lock);

  const SysxctLockEntry* get_array() const { return array_; }
  SysxctLockEntry* get_array() { return array_; }
  SysxctLockEntry* get_entry(LockListPosition pos) {
    ASSERT_ND(is_valid_entry(pos));
    return array_ + pos;
  }
  const SysxctLockEntry* get_entry(LockListPosition pos) const {
    ASSERT_ND(is_valid_entry(pos));
    return array_ + pos;
  }
  uint32_t  get_capacity() const { return kMaxSysxctLocks; }
  /** When this returns full, it's catastrophic. */
  bool      is_full() const { return last_active_entry_ == kMaxSysxctLocks; }
  LockListPosition get_last_active_entry() const { return last_active_entry_; }
  bool is_valid_entry(LockListPosition pos) const {
    return pos != kLockListPositionInvalid && pos <= last_active_entry_;
  }
  bool is_empty() const { return last_active_entry_ == kLockListPositionInvalid; }

  friend std::ostream& operator<<(std::ostream& o, const SysxctLockList& v);
  void assert_sorted() const ALWAYS_INLINE;
  void assert_sorted_impl() const;

  SysxctLockEntry* begin() { return array_ + 1U; }
  SysxctLockEntry* end() { return array_ + 1U + last_active_entry_; }
  const SysxctLockEntry* cbegin() const { return array_ + 1U; }
  const SysxctLockEntry* cend() const { return array_ + 1U + last_active_entry_; }
  /**
   * @returns largest index of entries that are already locked.
   * kLockListPositionInvalid if no entry is locked.
   */
  LockListPosition get_last_locked_entry() const { return last_locked_entry_; }
  /** Calculate last_locked_entry_ by really checking the whole list. Usually for sanity checks */
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

  UniversalLockId get_enclosing_max_lock_id() const { return enclosing_max_lock_id_; }
  /** @return whether the lock must be taken in try mode */
  bool is_try_mode_required(LockListPosition pos) const {
    const SysxctLockEntry* entry = get_entry(pos);
    // If the enclosing thread took a lock after this, we must be careful.
    if (entry->universal_lock_id_ <= enclosing_max_lock_id_) {
      return true;
    }

    // If this list contains an already-taken lock after this, we must be careful.
    if (pos <= last_locked_entry_) {
      return true;
    }

    return false;
  }

 private:
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

  /**
   * The largest lock Id that is taken by the enclosing normal transaction thread.
   * 0 means there is no such lock.
   * Because our sysxct takes both page and record locks, we must be careful on deadlock.
   * This value is given by the enclosing thread to tell which locks a sysxct should take
   * in try mode (all locks BEFORE this ID must be in try mode).
   */
  UniversalLockId enclosing_max_lock_id_;

  /**
   * Array of lock entries in the current run.
   * Index-0 is reserved as a dummy entry, so array_[1] and onwards are used.
   * Entries are distinct.
   */
  SysxctLockEntry array_[kMaxSysxctLocks];

  LockListPosition issue_new_position() {
    assert_last_locked_entry();
    ++last_active_entry_;
    ASSERT_ND(last_active_entry_ < get_capacity());
    return last_active_entry_;
  }

  /** Acquires locks of the given range. Be aware that from is pos, last is lock-id. */
  template<typename MCS_ADAPTOR>
  ErrorCode try_or_acquire_multiple_locks(
    MCS_ADAPTOR mcs_adaptor,
    LockListPosition from_pos,
    UniversalLockId last_lock_id,
    bool mandatory_locks);

  template<typename MCS_ADAPTOR>
  ErrorCode request_lock_general(
    MCS_ADAPTOR mcs_adaptor,
    storage::VolatilePagePointer page_id,
    uintptr_t lock,
    bool page_lock) {
    // Just reuse the batched version. Shouldn't have any significant overhead.
    // Using the same codepath is better also for testing/robustness. Not just I'm lazy!
    return batch_request_locks_general(mcs_adaptor, page_id, 1U, &lock, page_lock);
  }

  template<typename MCS_ADAPTOR>
  ErrorCode batch_request_locks_general(
    MCS_ADAPTOR mcs_adaptor,
    storage::VolatilePagePointer page_id,
    uint32_t lock_count,
    uintptr_t* locks,
    bool page_lock);
};

/**
 * @brief Per-thread reused work memory for system transactions.
 * @ingroup SYSXCT
 * @details
 * This object is a POD. It is maintained as part of ThreadPimpl for each thread.
 *
 * System transactions are not exposed to users, so the whole file is _impl.
 */
struct SysxctWorkspace {
  /**
   * Whether we are already running a sysxct using this workspace.
   * We don't allow more than one sysxct using this workspace.
   */
  bool running_sysxct_;

  char pad_[64U - sizeof(running_sysxct_)];

  /** Lock list for the system transaction */
  SysxctLockList lock_list_;

  void init() {
    running_sysxct_ = false;
    lock_list_.init();
  }

  friend std::ostream& operator<<(std::ostream& o, const SysxctWorkspace& v);
};

/**
 * @brief Runs a system transaction nested in a user transaction.
 * @param[in] context the enclosing user transaction's thread.
 * @param[in] functor the functor content of the system transaction.
 * @param[in] mcs_adaptor abstracts MCS interface.
 * @param[in] max_retries How many times at most to retry the same system transaction if
 * it aborts. 0 means no retries (default behavior). It retries when the return value
 * of the functor was RaceAbort or LockAbort. Other return values are immediately propagated.
 * @param[in,out] workspace Resource for system transaction.
 * @param[in] enclosing_max_lock_id largest lock-id held by outer transaction.
 * @param[in] enclosure_release_all_locks_functor when the system transaction gets a race abort,
 * it might call this back to release all locks in the outer transaction.
 * @return Propagates the return value of the functor.
 * @ingroup SYSXCT
 * @details
 * System transactions are much more casual transactions than user transactions.
 * They are begun/committed for a small function or a few. In order to make sure
 * we release related resources in SysxctWorkspace, we should always use this function.
 *
 * This function is for the majority of system transaction usecases where
 * a normal user transaction needs to physically change some page/record and
 * invokes a system transaction.
 */
template<typename MCS_ADAPTOR, typename ENCLOSURE_RELEASE_ALL_LOCKS_FUNCTOR>
inline ErrorCode run_nested_sysxct_impl(
  SysxctFunctor* functor,
  MCS_ADAPTOR mcs_adaptor,
  uint32_t max_retries,
  SysxctWorkspace* workspace,
  UniversalLockId enclosing_max_lock_id,
  ENCLOSURE_RELEASE_ALL_LOCKS_FUNCTOR enclosure_release_all_locks_functor) {
  if (UNLIKELY(workspace->running_sysxct_)) {
    // so far each thread can convey only one system transaction.
    // this is 1) to reuse SysxctWorkspace, and 2) to avoid deadlocks.
    return kErrorCodeXctTwoSysXcts;
  }

  workspace->running_sysxct_ = true;
  uint32_t cur_retries = 0;
  auto* lock_list = &(workspace->lock_list_);
  lock_list->clear_entries(enclosing_max_lock_id);
  while (true) {
    ErrorCode ret = functor->run();
    // In any case, release all locks.
    lock_list->release_all_locks(mcs_adaptor);

    // The first run of the system transaction takes locks only in "try" mode
    // to avoid deadlocks. They thus have higher chance to get aborted.
    if (UNLIKELY(ret == kErrorCodeXctRaceAbort || ret == kErrorCodeXctLockAbort)) {
      // consider retry
      if (cur_retries < max_retries) {
        // When we retry, we release all locks in _outer_ transaction to eliminate the
        // chance of deadlocks. Then the next run of the system transaction can
        // take locks unconditionally, waiting for an arbitrary period.
        if (enclosing_max_lock_id != kNullUniversalLockId) {
          enclosure_release_all_locks_functor();
          lock_list->compress_entries(kNullUniversalLockId);
          enclosing_max_lock_id = kNullUniversalLockId;
          // Now the next run, helped by inherited entries, should be in unconditional mode
        }
        ++cur_retries;
        continue;
      }
    }

    workspace->running_sysxct_ = false;
    return ret;
  }
}


// TASK(Hideaki) standalone sysxct... Needed in a few places.


////////////////////////////////////////////////////////////////////////
/// Inline definitions of SysxctLockList methods
////////////////////////////////////////////////////////////////////////
inline void SysxctLockList::assert_sorted() const {
  // In release mode, this code must be completely erased by compiler
#ifndef  NDEBUG
  assert_sorted_impl();
#endif  // NDEBUG
}

template<typename MCS_ADAPTOR>
inline ErrorCode SysxctLockList::try_or_acquire_multiple_locks(
  MCS_ADAPTOR mcs_adaptor,
  LockListPosition from_pos,
  UniversalLockId last_lock_id,
  bool mandatory_locks) {
  ASSERT_ND(from_pos != kLockListPositionInvalid);
  assert_sorted();
  for (LockListPosition pos = from_pos;; ++pos) {
    if (pos > last_active_entry_) {
      break;
    } else if (array_[pos].universal_lock_id_ > last_lock_id) {
      // The only use of this func is batch_request_locks_general, where last_lock_id surely exists
      ASSERT_ND(array_[pos - 1].universal_lock_id_ == last_lock_id);
      break;
    }

    SysxctLockEntry* lock_entry = get_entry(pos);
    if (mandatory_locks) {
      lock_entry->used_in_this_run_ = true;
    }
    if (lock_entry->is_locked()) {
      continue;
    }

    const bool needs_try = is_try_mode_required(pos);
    if (lock_entry->page_lock_) {
      McsWwImpl< MCS_ADAPTOR > impl(mcs_adaptor);
      storage::Page* page = lock_entry->get_as_page_lock();
      McsWwLock* lock_addr = &page->get_header().page_version_.lock_;
      if (needs_try) {
        lock_entry->mcs_block_ = impl.acquire_try(lock_addr);
      } else {
        lock_entry->mcs_block_ = impl.acquire_unconditional(lock_addr);
        ASSERT_ND(lock_entry->is_locked());
      }
    } else {
      McsImpl< MCS_ADAPTOR, typename MCS_ADAPTOR::ThisRwBlock > impl(mcs_adaptor);
      McsRwLock* lock_addr = &lock_entry->get_as_record_lock()->lock_;
      if (needs_try) {
        lock_entry->mcs_block_ = impl.acquire_try_rw_writer(lock_addr);
      } else {
        lock_entry->mcs_block_ = impl.acquire_unconditional_rw_writer(lock_addr);
        ASSERT_ND(lock_entry->is_locked());
      }
    }
    if (lock_entry->mcs_block_) {
      if (last_locked_entry_ < pos) {
        last_locked_entry_ = pos;
      }
      assert_last_locked_entry();
    } else {
      return kErrorCodeXctRaceAbort;
    }
  }

  return kErrorCodeOk;
}

template<typename MCS_ADAPTOR>
inline ErrorCode SysxctLockList::batch_request_locks_general(
  MCS_ADAPTOR mcs_adaptor,
  storage::VolatilePagePointer page_id,
  uint32_t lock_count,
  uintptr_t* locks,
  bool page_lock) {
  if (lock_count == 0) {
    return kErrorCodeOk;
  }

  // First, put corresponding entries to the list. either existing or just confirming.
  LockListPosition from_pos = batch_get_or_add_entries(page_id, lock_count, locks, page_lock);
  if (from_pos > 1U) {
    // To reduce the chance of deadlocks, we take locks before these locks.
    // These are non-mandatory locks, so we don't turn on the "used_in_this_run_" flags
    UniversalLockId upto = to_universal_lock_id(page_id, locks[0]) - 1U;
    CHECK_ERROR_CODE(try_or_acquire_multiple_locks<MCS_ADAPTOR>(mcs_adaptor, 1U, upto, false));
  }


  // Upto which lock ID are we locking? Note, this param is lock-id, not pos.
  UniversalLockId last_lock_id = to_universal_lock_id(page_id, locks[lock_count - 1]);

  // Then, lock them all! These are "mandatory" locks.
  return try_or_acquire_multiple_locks<MCS_ADAPTOR>(mcs_adaptor, from_pos, last_lock_id, true);
}

template<typename MCS_ADAPTOR>
inline void SysxctLockList::release_all_locks(MCS_ADAPTOR mcs_adaptor) {
  for (auto* entry = begin(); entry != end(); ++entry) {
    if (!entry->is_locked()) {
      continue;
    }
    if (entry->page_lock_) {
      McsWwImpl< MCS_ADAPTOR > impl(mcs_adaptor);
      storage::Page* page = entry->get_as_page_lock();
      McsWwLock* lock_addr = &page->get_header().page_version_.lock_;
      impl.release(lock_addr, entry->mcs_block_);
      entry->mcs_block_ = 0;
    } else {
      McsImpl< MCS_ADAPTOR, typename MCS_ADAPTOR::ThisRwBlock > impl(mcs_adaptor);
      McsRwLock* lock_addr = &entry->get_as_record_lock()->lock_;
      impl.release_rw_writer(lock_addr, entry->mcs_block_);
      entry->mcs_block_ = 0;
    }
  }
  last_locked_entry_ = kLockListPositionInvalid;
}

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_SYSXCT_IMPL_HPP_
