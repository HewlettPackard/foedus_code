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
#include "foedus/xct/retrospective_lock_list.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>

#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"

namespace foedus {
namespace xct {

////////////////////////////////////////////////////////////
/// Init/Uninit
////////////////////////////////////////////////////////////
RetrospectiveLockList::RetrospectiveLockList() {
  array_ = nullptr;
  capacity_ = 0;
  clear_entries();
}

RetrospectiveLockList::~RetrospectiveLockList() {
  uninit();
}

void RetrospectiveLockList::init(
  LockEntry* array,
  uint32_t capacity,
  const memory::GlobalVolatilePageResolver& resolver) {
  array_ = array;
  capacity_ = capacity;
  volatile_page_resolver_ = resolver;
  clear_entries();
}

void RetrospectiveLockList::clear_entries() {
  last_active_entry_ = kLockListPositionInvalid;
  if (array_) {
    // Dummy entry
    array_[kLockListPositionInvalid].universal_lock_id_ = 0;
    array_[kLockListPositionInvalid].lock_ = nullptr;
    array_[kLockListPositionInvalid].preferred_mode_ = kNoLock;
    array_[kLockListPositionInvalid].taken_mode_ = kNoLock;
  }
}

void RetrospectiveLockList::uninit() {
  array_ = nullptr;
  capacity_ = 0;
  clear_entries();
}

CurrentLockList::CurrentLockList() {
  array_ = nullptr;
  capacity_ = 0;
  clear_entries();
}

CurrentLockList::~CurrentLockList() {
  uninit();
}

void CurrentLockList::init(
  LockEntry* array,
  uint32_t capacity,
  const memory::GlobalVolatilePageResolver& resolver) {
  array_ = array;
  capacity_ = capacity;
  volatile_page_resolver_ = resolver;
  clear_entries();
}

void CurrentLockList::clear_entries() {
  last_active_entry_ = kLockListPositionInvalid;
  if (array_) {
    // Dummy entry
    array_[kLockListPositionInvalid].universal_lock_id_ = 0;
    array_[kLockListPositionInvalid].lock_ = nullptr;
    array_[kLockListPositionInvalid].preferred_mode_ = kNoLock;
    array_[kLockListPositionInvalid].taken_mode_ = kNoLock;
  }
}


void CurrentLockList::uninit() {
  array_ = nullptr;
  capacity_ = 0;
  clear_entries();
}

////////////////////////////////////////////////////////////
/// Debugging
////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& o, const LockEntry& v) {
  o << "<LockEntry>"
    << "<LockId>" << v.universal_lock_id_ << "</LockId>"
    << "<PreferredMode>" << v.preferred_mode_ << "</PreferredMode>"
    << "<TakenMode>" << v.taken_mode_ << "</TakenMode>";
  if (v.lock_) {
    o << *(v.lock_);
  } else {
    o << "<Lock>nullptr</Lock>";
  }
  o << "</LockEntry>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const CurrentLockList& v) {
  o << "<CurrentLockList>"
    << "<Capacity>" << v.capacity_ << "</Capacity>"
    << "<LastActiveEntry>" << v.last_active_entry_ << "</LastActiveEntry>";
  const uint32_t kMaxShown = 32U;
  for (auto i = 1U; i <= std::min(v.last_active_entry_, kMaxShown); ++i) {
    o << v.array_[i];
  }
  o << "</CurrentLockList>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const RetrospectiveLockList& v) {
  o << "<RetrospectiveLockList>"
    << "<Capacity>" << v.capacity_ << "</Capacity>"
    << "<LastActiveEntry>" << v.last_active_entry_ << "</LastActiveEntry>";
  const uint32_t kMaxShown = 32U;
  for (auto i = 1U; i <= std::min(v.last_active_entry_, kMaxShown); ++i) {
    o << v.array_[i];
  }
  o << "</RetrospectiveLockList>";
  return o;
}


template<typename LOCK_LIST>
void lock_assert_sorted(const memory::GlobalVolatilePageResolver& resolver, const LOCK_LIST& list) {
  const LockEntry* array = list.get_array();
  ASSERT_ND(array[kLockListPositionInvalid].universal_lock_id_ == 0);
  ASSERT_ND(array[kLockListPositionInvalid].lock_ == nullptr);
  ASSERT_ND(array[kLockListPositionInvalid].taken_mode_ == kNoLock);
  ASSERT_ND(array[kLockListPositionInvalid].preferred_mode_ == kNoLock);
  const LockListPosition last_active_entry = list.get_last_active_entry();
  for (LockListPosition pos = 2U; pos <= last_active_entry; ++pos) {
    ASSERT_ND(array[pos - 1U].universal_lock_id_ < array[pos].universal_lock_id_);
    ASSERT_ND(array[pos].universal_lock_id_ != 0);
    ASSERT_ND(array[pos].lock_ != nullptr);
    ASSERT_ND(array[pos].universal_lock_id_
      == xct_id_to_universal_lock_id(resolver, array[pos].lock_));
    ASSERT_ND(array[pos].lock_ == from_universal_lock_id(resolver, array[pos].universal_lock_id_));
  }
}

void CurrentLockList::assert_sorted_impl() const {
  lock_assert_sorted(volatile_page_resolver_, *this);
}
void RetrospectiveLockList::assert_sorted_impl() const {
  lock_assert_sorted(volatile_page_resolver_, *this);
}

////////////////////////////////////////////////////////////
/// Data manipulation (search/add/etc)
/// These implementations are skewed towards sorted cases,
/// meaning it runs faster when accesses are nicely sorted.
////////////////////////////////////////////////////////////
template<typename LOCK_LIST>
LockListPosition lock_lower_bound(
  const LOCK_LIST& list,
  UniversalLockId lock) {
  LockListPosition last_active_entry = list.get_last_active_entry();
  if (last_active_entry == kLockListPositionInvalid) {
    return kLockListPositionInvalid + 1U;
  }
  // Check the easy cases first. This will be an wasted cost if it's not, but still cheap.
  const LockEntry* array = list.get_array();
  // For example, [dummy, 3, 5, 7] (last_active_entry=3).
  // id=7: 3, larger: 4, smaller: need to check more
  if (array[last_active_entry].universal_lock_id_ == lock) {
    return last_active_entry;
  } else if (array[last_active_entry].universal_lock_id_ < lock) {
    return last_active_entry + 1U;
  }

  DVLOG(2) << "not an easy case. Binary search!";
  LockListPosition pos
    = std::lower_bound(
        array + 1U,
        array + last_active_entry + 1U,
        lock,
        LockEntry::LessThan())
      - array;
  // in the above example, id=6: 3, id=4,5: 2, smaller: 1
  ASSERT_ND(pos != kLockListPositionInvalid);
  ASSERT_ND(pos <= last_active_entry);  // otherwise we went into the branch above
  ASSERT_ND(array[pos].universal_lock_id_ >= lock);
  ASSERT_ND(pos == 1U || array[pos - 1U].universal_lock_id_ < lock);
  return pos;
}

template<typename LOCK_LIST>
LockListPosition lock_binary_search(
  const LOCK_LIST& list,
  UniversalLockId lock) {
  LockListPosition last_active_entry = list.get_last_active_entry();
  LockListPosition pos = lock_lower_bound<LOCK_LIST>(list, lock);
  if (pos != kLockListPositionInvalid && pos <= last_active_entry) {
    const LockEntry* array = list.get_array();
    if (array[pos].universal_lock_id_ == lock) {
      return pos;
    }
  }
  return kLockListPositionInvalid;
}

LockListPosition CurrentLockList::binary_search(UniversalLockId lock) const {
  return lock_binary_search<CurrentLockList>(*this, lock);
}
LockListPosition RetrospectiveLockList::binary_search(UniversalLockId lock) const {
  return lock_binary_search<RetrospectiveLockList>(*this, lock);
}
LockListPosition CurrentLockList::lower_bound(UniversalLockId lock) const {
  return lock_lower_bound<CurrentLockList>(*this, lock);
}
LockListPosition RetrospectiveLockList::lower_bound(UniversalLockId lock) const {
  return lock_lower_bound<RetrospectiveLockList>(*this, lock);
}

LockListPosition CurrentLockList::get_or_add_entry(
  RwLockableXctId* lock, LockMode preferred_mode) {
  // Easy case? (lock >= the last entry)
  const UniversalLockId id = xct_id_to_universal_lock_id(volatile_page_resolver_, lock);
  LockListPosition insert_pos = lower_bound(id);
  ASSERT_ND(insert_pos != kLockListPositionInvalid);

  // Larger than all existing entries? Append to the last!
  if (insert_pos > last_active_entry_) {
    ASSERT_ND(insert_pos == last_active_entry_ + 1U);
    LockListPosition new_pos = issue_new_position();
    array_[new_pos].set(id, lock, preferred_mode, kNoLock);
    ASSERT_ND(new_pos == insert_pos);
    return new_pos;
  }

  // lower_bound returns the first entry that is NOT less than. is it equal?
  ASSERT_ND(array_[insert_pos].universal_lock_id_ >= id);
  if (array_[insert_pos].universal_lock_id_ == id) {
    // Found existing!
    if (array_[insert_pos].preferred_mode_ < preferred_mode) {
      array_[insert_pos].preferred_mode_ = preferred_mode;
    }
    return insert_pos;
  }

  DVLOG(1) << "not an easy case. We need to adjust the order. This is costly!";
  ASSERT_ND(insert_pos <= last_active_entry_);  // otherwise we went into the 1st branch
  ASSERT_ND(array_[insert_pos].universal_lock_id_ > id);  // if ==, we went into the 2nd branch
  ASSERT_ND(insert_pos == 1U || array_[insert_pos - 1U].universal_lock_id_ < id);

  LockListPosition new_last_pos = issue_new_position();
  ASSERT_ND(new_last_pos > insert_pos);
  uint64_t moved_bytes = sizeof(LockEntry) * (new_last_pos - insert_pos);
  std::memmove(array_ + insert_pos + 1U, array_ + insert_pos, moved_bytes);
  DVLOG(1) << "Re-sorted. hope this won't happen often";
  array_[insert_pos].set(id, lock, preferred_mode, kNoLock);
  assert_sorted();
  return insert_pos;
}

void RetrospectiveLockList::construct(thread::Thread* context, uint32_t read_lock_threshold) {
  Xct* xct = &context->get_current_xct();
  ASSERT_ND(xct->is_active());
  // We currently hold read/write-set separately. So, we need to sort and merge them.
  ReadXctAccess* read_set = xct->get_read_set();
  const uint32_t read_set_size = xct->get_read_set_size();
  WriteXctAccess* write_set = xct->get_write_set();
  const uint32_t write_set_size = xct->get_write_set_size();
  ASSERT_ND(capacity_ >= read_set_size + write_set_size);

  last_active_entry_ = kLockListPositionInvalid;
  for (uint32_t i = 0; i < read_set_size; ++i) {
    RwLockableXctId* lock = read_set[i].owner_id_address_;
    storage::Page* page = storage::to_page(lock);
    if (page->get_header().hotness_.value_ < read_lock_threshold
      && lock->xct_id_ == read_set[i].observed_owner_id_) {
      // We also add it to RLL whenever we observed a verification error.
      continue;
    }

    auto pos = issue_new_position();
    array_[pos].set(
      xct_id_to_universal_lock_id(volatile_page_resolver_, lock), lock, kReadLock, kNoLock);
  }
  DVLOG(1) << "Added " << last_active_entry_ << " to RLL for read-locks";

  // Writes are always added to RLL.
  for (uint32_t i = 0; i < write_set_size; ++i) {
    auto pos = issue_new_position();
    ASSERT_ND(
      write_set[i].owner_lock_id_ ==
      xct_id_to_universal_lock_id(volatile_page_resolver_, write_set[i].owner_id_address_));
    array_[pos].set(
      write_set[i].owner_lock_id_,
      write_set[i].owner_id_address_,
      kWriteLock,
      kNoLock);
  }

  // Now, the entries are not sorted and we might have duplicates.
  // Sort them, and merge entries for the same record.
  // std::set? no joke. we can't afford heap allocation here.
  std::sort(array_ + 1U, array_ + last_active_entry_ + 1U);
  LockListPosition prev_pos = 1U;
  uint32_t merged_count = 0U;
  for (LockListPosition pos = 2U; pos <= last_active_entry_; ++pos) {
    ASSERT_ND(prev_pos < pos);
    ASSERT_ND(array_[prev_pos].universal_lock_id_ <= array_[pos].universal_lock_id_);
    if (array_[prev_pos].universal_lock_id_ == array_[pos].universal_lock_id_) {
      // Merge!
      if (array_[pos].preferred_mode_ == kWriteLock) {
        array_[prev_pos].preferred_mode_ = kWriteLock;
      }
      ++merged_count;
    } else {
      // No merge.
      if (prev_pos + 1U < pos) {
        std::memcpy(array_ + prev_pos + 1U, array_ + pos, sizeof(LockEntry));
      }
      ++prev_pos;
    }
  }

  // For example, last_active_entry_ was 3 (0=Dummy, 1=A, 2=A, 3=B),
  // prev_pos becomes 2 while merged count is 1.
  ASSERT_ND(prev_pos + merged_count == last_active_entry_);
  last_active_entry_ = prev_pos;
  assert_sorted();
}

void CurrentLockList::batch_insert_write_placeholders(
  const WriteXctAccess* write_set,
  uint32_t write_set_size) {
  // We want to avoid full-sorting and minimize the number of copies/shifts.
  // Luckily, write_set is already sorted, so is our array_. Just merge them in order.
  if (write_set_size == 0) {
    return;
  }
#ifndef NDEBUG
  for (uint32_t i = 1; i < write_set_size; ++i) {
    ASSERT_ND(write_set[i - 1].write_set_ordinal_ != write_set[i].write_set_ordinal_);
    if (write_set[i].owner_lock_id_ == write_set[i - 1].owner_lock_id_) {
      ASSERT_ND(write_set[i - 1].write_set_ordinal_ < write_set[i].write_set_ordinal_);
    } else {
      ASSERT_ND(write_set[i - 1].owner_lock_id_ < write_set[i].owner_lock_id_);
    }
  }
  assert_sorted();
#endif  // NDEBUG

  // Implementation note: I considered a few approaches to efficiently do the merge.
  //  1) insertion-sort: that sounds expensive... we might be inserting several
  //  2) a bit complex. first path to identify the number of new entries, then second path to
  //   merge from the end, not the beginning, to copy/shift only what we need to.
  //  3) insert all write-sets at the end then invoke std::sort once.
  // For now I picked 3) for simplicity. Revisit laster if CPU profile tells something.
  uint32_t write_pos = 0;
  uint32_t added = 0;
  for (LockListPosition pos = 1U; pos <= last_active_entry_ && write_pos < write_set_size;) {
    LockEntry* existing = array_ + pos;
    const WriteXctAccess* write = write_set + write_pos;
    UniversalLockId write_lock_id = write->owner_lock_id_;
    if (existing->universal_lock_id_ < write_lock_id) {
      ++pos;
    } else if (existing->universal_lock_id_ == write_lock_id) {
      if (existing->preferred_mode_ != kWriteLock) {
        existing->preferred_mode_ = kWriteLock;
      }
      ++write_pos;
    } else {
      // yuppy, new entry.
      ASSERT_ND(existing->universal_lock_id_ > write_lock_id);
      ++added;
      LockEntry* new_entry = array_ + last_active_entry_ + added;
      new_entry->set(write_lock_id, write->owner_id_address_, kWriteLock, kNoLock);
      // be careful on duplicate in write-set.
      // It might contain multiple writes to one record.
      for (++write_pos; write_pos < write_set_size; ++write_pos) {
        const WriteXctAccess* next_write = write_set + write_pos;
        UniversalLockId next_write_id = next_write->owner_lock_id_;
        ASSERT_ND(next_write_id >= write_lock_id);
        if (next_write_id > write_lock_id) {
          break;
        }
      }
    }
  }

  while (write_pos < write_set_size) {
    // After iterating over all existing entries, still some write-set entry remains.
    // Hence they are all after the existing entries.
    const WriteXctAccess* write = write_set + write_pos;
    UniversalLockId write_lock_id = write->owner_lock_id_;
    ASSERT_ND(last_active_entry_ == kLockListPositionInvalid
      || array_[last_active_entry_].universal_lock_id_ < write_lock_id);

    // Again, be careful on duplicate in write set.
    ++added;
    LockEntry* new_entry = array_ + last_active_entry_ + added;
    new_entry->set(write_lock_id, write->owner_id_address_, kWriteLock, kNoLock);
    for (++write_pos; write_pos < write_set_size; ++write_pos) {
      const WriteXctAccess* next_write = write_set + write_pos;
      UniversalLockId next_write_id = next_write->owner_lock_id_;
      ASSERT_ND(next_write_id >= write_lock_id);
      if (next_write_id > write_lock_id) {
        break;
      }
    }
  }

  if (added > 0) {
    last_active_entry_ += added;
    std::sort(array_ + 1U, array_ + 1U + last_active_entry_);
  }
  assert_sorted();
#ifndef NDEBUG
  for (uint32_t i = 0; i < write_set_size; ++i) {
    ASSERT_ND(binary_search(write_set[i].owner_lock_id_) != kLockListPositionInvalid);
  }
#endif  // NDEBUG
}

void CurrentLockList::prepopulate_for_retrospective_lock_list(const RetrospectiveLockList& rll) {
  ASSERT_ND(is_empty());
  ASSERT_ND(!rll.is_empty());
  rll.assert_sorted();
  // Because now we use LockEntry for both RLL and CLL, we can do just one memcpy
  std::memcpy(array_ + 1U, rll.get_array() + 1U, sizeof(LockEntry) * rll.get_last_active_entry());
  last_active_entry_ = rll.get_last_active_entry();
  assert_sorted();
}

ErrorCode CurrentLockList::try_or_acquire_single_lock_impl(
  thread::Thread* context,
  LockListPosition pos,
  LockListPosition* last_locked_pos) {
  LockEntry* lock_entry = get_entry(pos);
  if (lock_entry->is_enough()) {
    return kErrorCodeOk;
  }
  ASSERT_ND(lock_entry->taken_mode_ != kWriteLock);

  auto* lock_addr = lock_entry->lock_->get_key_lock();
  if (lock_entry->taken_mode_ != kNoLock) {
    ASSERT_ND(lock_entry->preferred_mode_ == kWriteLock);
    ASSERT_ND(lock_entry->taken_mode_ == kReadLock);
    ASSERT_ND(lock_entry->mcs_block_);
    // This is reader->writer upgrade.
    // We simply release read-lock first then take write-lock in this case.
    // In traditional 2PL, such an unlock-then-lock violates serializability,
    // but we guarantee serializability by read-verification anyways.
    // We can release any lock anytime.. great flexibility!
    context->mcs_release_reader_lock(lock_addr, lock_entry->mcs_block_);

    // simply recalculate. We can do a bit smarter thing.. if CPU profile tells something.
    // unlikely because lock upgrade shouldn't be that often. RLL should minimize it.
    *last_locked_pos = get_last_locked_entry();
  } else {
    // This method is for unconditional acquire and try, not aync/retry.
    // If we have a queue node already, something was misused.
    ASSERT_ND(lock_entry->mcs_block_ == 0);
  }

  // Now we need to take the lock. Are we in canonical mode?
  if (*last_locked_pos == kLockListPositionInvalid || *last_locked_pos < pos) {
    // yay, we are in canonical mode. we can unconditionally get the lock
    ASSERT_ND(lock_entry->taken_mode_ == kNoLock);  // not a lock upgrade, either
    if (lock_entry->preferred_mode_ == kWriteLock) {
      lock_entry->mcs_block_ = context->mcs_acquire_writer_lock(lock_addr);
    } else {
      ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
      lock_entry->mcs_block_ = context->mcs_acquire_reader_lock(lock_addr);
    }
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
  } else {
    // hmm, we violated canonical mode. has a risk of deadlock.
    // Let's just try acquire the lock and immediately give up if it fails.
    // The RLL will take care of the next run.
    // TODO(Hideaki) release some of the lock we have taken to restore canonical mode.
    // We haven't imlpemented this optimization yet.
    ASSERT_ND(lock_entry->mcs_block_ == 0);
    ASSERT_ND(lock_entry->taken_mode_ == kNoLock);
    if (lock_entry->preferred_mode_ == kWriteLock) {
      lock_entry->mcs_block_ = context->mcs_try_acquire_writer_lock(lock_addr);
    } else {
      ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
      lock_entry->mcs_block_ = context->mcs_try_acquire_reader_lock(lock_addr);
    }
    if (lock_entry->mcs_block_ == 0) {
      DVLOG(0) << "Failed to try-acquire a lock.";
      return kErrorCodeXctLockAbort;
    }
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
  }
  ASSERT_ND(lock_entry->mcs_block_);
  ASSERT_ND(lock_entry->is_locked());
  *last_locked_pos = std::max(pos, *last_locked_pos);
  return kErrorCodeOk;
}

void CurrentLockList::try_async_single_lock(
  thread::Thread* context,
  LockListPosition pos) {
  LockEntry* lock_entry = get_entry(pos);
  if (lock_entry->is_enough()) {
    return;
  }
  ASSERT_ND(lock_entry->taken_mode_ != kWriteLock);

  auto* lock_addr = lock_entry->lock_->get_key_lock();
  if (lock_entry->taken_mode_ != kNoLock) {
    ASSERT_ND(lock_entry->preferred_mode_ == kWriteLock);
    ASSERT_ND(lock_entry->taken_mode_ == kReadLock);
    ASSERT_ND(lock_entry->mcs_block_);
    // This is reader->writer upgrade.
    // We simply release read-lock first then take write-lock in this case.
    // In traditional 2PL, such an unlock-then-lock violates serializability,
    // but we guarantee serializability by read-verification anyways.
    // We can release any lock anytime.. great flexibility!
    context->mcs_release_reader_lock(lock_addr, lock_entry->mcs_block_);
    lock_entry->taken_mode_ = kNoLock;
  } else {
    // This function is for pushing the queue node in the extended rwlock.
    // Doomed if we already have a queue node.
    ASSERT_ND(lock_entry->mcs_block_ == 0);
  }

  // Don't really care canonical order here, just send out the request.
  AcquireAsyncRet async_ret;
  if (lock_entry->preferred_mode_ == kWriteLock) {
    async_ret = context->mcs_acquire_async_rw_writer(lock_addr);
  } else {
    ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
    async_ret = context->mcs_acquire_async_rw_reader(lock_addr);
  }
  ASSERT_ND(async_ret.block_index_);
  lock_entry->mcs_block_ = async_ret.block_index_;
  if (async_ret.acquired_) {
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
    ASSERT_ND(lock_entry->is_enough());
  }
  ASSERT_ND(lock_entry->mcs_block_);
}

bool CurrentLockList::retry_async_single_lock(
  thread::Thread* context,
  LockListPosition pos) {
  LockEntry* lock_entry = get_entry(pos);
  // Must be not taken yet, and must have pushed a qnode to the lock queue
  ASSERT_ND(!lock_entry->is_enough());
  ASSERT_ND(lock_entry->taken_mode_ == kNoLock);
  ASSERT_ND(lock_entry->mcs_block_);
  ASSERT_ND(!lock_entry->is_locked());

  auto* lock_addr = lock_entry->lock_->get_key_lock();
  bool acquired = false;
  if (lock_entry->preferred_mode_ == kWriteLock) {
    acquired = context->mcs_retry_async_rw_writer(lock_addr, lock_entry->mcs_block_);
  } else {
    ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
    acquired = context->mcs_retry_async_rw_reader(lock_addr, lock_entry->mcs_block_);
  }
  if (acquired) {
    lock_entry->taken_mode_ = lock_entry->preferred_mode_;
    ASSERT_ND(lock_entry->is_locked());
  }
  return acquired;
}

void CurrentLockList::cancel_async_single_lock_impl(
  thread::Thread* context,
  LockListPosition pos) {
  LockEntry* lock_entry = get_entry(pos);
  ASSERT_ND(!lock_entry->is_enough());
  ASSERT_ND(lock_entry->taken_mode_ == kNoLock);
  ASSERT_ND(lock_entry->mcs_block_);
  auto* lock_addr = lock_entry->lock_->get_key_lock();
  if (lock_entry->preferred_mode_ == kReadLock) {
    context->mcs_cancel_async_rw_reader(lock_addr, lock_entry->mcs_block_);
  } else {
    ASSERT_ND(lock_entry->preferred_mode_ == kReadLock);
    context->mcs_cancel_async_rw_writer(lock_addr, lock_entry->mcs_block_);
  }
  lock_entry->mcs_block_ = 0;
}

ErrorCode CurrentLockList::try_or_acquire_single_lock(
  thread::Thread* context,
  LockListPosition pos) {
  LockListPosition last_locked_pos = get_last_locked_entry();
  return try_or_acquire_single_lock_impl(context, pos, &last_locked_pos);
}

void CurrentLockList::cancel_async_single_lock(
  thread::Thread* context,
  LockListPosition pos) {
  cancel_async_single_lock_impl(context, pos);
}

ErrorCode CurrentLockList::try_or_acquire_multiple_locks(
  thread::Thread* context,
  LockListPosition upto_pos) {
  ASSERT_ND(upto_pos != kLockListPositionInvalid);
  ASSERT_ND(upto_pos <= last_active_entry_);
  LockListPosition last_locked_pos = get_last_locked_entry();
  // Especially in this case, we probably should release locks after upto_pos first.
  for (LockListPosition pos = 1U; pos <= upto_pos; ++pos) {
    CHECK_ERROR_CODE(try_or_acquire_single_lock_impl(context, pos, &last_locked_pos));
  }
  return kErrorCodeOk;
}

void CurrentLockList::try_async_multiple_locks(
  thread::Thread* context,
  LockListPosition upto_pos) {
  ASSERT_ND(upto_pos != kLockListPositionInvalid);
  ASSERT_ND(upto_pos <= last_active_entry_);
  for (LockListPosition pos = 1U; pos <= upto_pos; ++pos) {
    try_async_single_lock(context, pos);
  }
}

}  // namespace xct
}  // namespace foedus
