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
#include "foedus/thread/thread_pimpl.hpp"       // only for explicit template instantiation
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_mcs_adapter_impl.hpp"  // only for explicit template instantiation
#include "foedus/xct/xct_mcs_impl.hpp"          // only for explicit template instantiation

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
  last_locked_entry_ = kLockListPositionInvalid;
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
    << "<LastActiveEntry>" << v.last_active_entry_ << "</LastActiveEntry>"
    << "<LastLockedEntry>" << v.last_locked_entry_ << "</LastLockedEntry>";
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
void lock_assert_sorted(const LOCK_LIST& list) {
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
    const storage::Page* page = storage::to_page(array[pos].lock_);
    uintptr_t lock_addr = reinterpret_cast<uintptr_t>(array[pos].lock_);
    auto page_id = page->get_volatile_page_id();
    ASSERT_ND(array[pos].universal_lock_id_
      == to_universal_lock_id(page_id.get_numa_node(), page_id.get_offset(), lock_addr));
  }
}

void CurrentLockList::assert_sorted_impl() const {
  assert_last_locked_entry();
  lock_assert_sorted(*this);
}
void RetrospectiveLockList::assert_sorted_impl() const {
  lock_assert_sorted(*this);
}

////////////////////////////////////////////////////////////
/// Data manipulation (search/add/etc)
////////////////////////////////////////////////////////////
LockListPosition CurrentLockList::binary_search(UniversalLockId lock) const {
  assert_last_locked_entry();
  return lock_binary_search<CurrentLockList, LockEntry>(*this, lock);
}
LockListPosition RetrospectiveLockList::binary_search(UniversalLockId lock) const {
  return lock_binary_search<RetrospectiveLockList, LockEntry>(*this, lock);
}
LockListPosition CurrentLockList::lower_bound(UniversalLockId lock) const {
  assert_last_locked_entry();
  return lock_lower_bound<CurrentLockList, LockEntry>(*this, lock);
}
LockListPosition RetrospectiveLockList::lower_bound(UniversalLockId lock) const {
  return lock_lower_bound<RetrospectiveLockList, LockEntry>(*this, lock);
}

LockListPosition CurrentLockList::get_or_add_entry(
  UniversalLockId id,
  RwLockableXctId* lock,
  LockMode preferred_mode) {
  assert_last_locked_entry();
  ASSERT_ND(id == xct_id_to_universal_lock_id(volatile_page_resolver_, lock));
  // Easy case? (lock >= the last entry)
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
  if (last_locked_entry_ >= insert_pos) {
    ++last_locked_entry_;
  }

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
    ASSERT_ND(
      read_set[i].owner_lock_id_ ==
      xct_id_to_universal_lock_id(volatile_page_resolver_, lock));
    array_[pos].set(read_set[i].owner_lock_id_, lock, kReadLock, kNoLock);
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

  if (last_active_entry_ == kLockListPositionInvalid) {
    return;
  }

  // Now, the entries are not sorted and we might have duplicates.
  // Sort them, and merge entries for the same record.
  // std::set? no joke. we can't afford heap allocation here.
  ASSERT_ND(last_active_entry_ != kLockListPositionInvalid);
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
    ASSERT_ND(write_set[i - 1].ordinal_ != write_set[i].ordinal_);
    if (write_set[i].owner_lock_id_ == write_set[i - 1].owner_lock_id_) {
      ASSERT_ND(write_set[i - 1].ordinal_ < write_set[i].ordinal_);
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
  if (last_active_entry_ == kLockListPositionInvalid) {
    // If CLL is now empty, it's even easier. Just add all write-sets
    uint32_t added = 0;
    for (uint32_t write_pos = 0; write_pos < write_set_size; ++write_pos) {
      const WriteXctAccess* write = write_set + write_pos;
      if (write_pos > 0) {
        const WriteXctAccess* prev = write_set + write_pos - 1;
        ASSERT_ND(write->ordinal_ != prev->ordinal_);
        ASSERT_ND(write->owner_lock_id_ >= prev->owner_lock_id_);
        if (write->owner_lock_id_ == prev->owner_lock_id_) {
          ASSERT_ND(write->ordinal_ > prev->ordinal_);
          continue;
        }
      }
      ++added;
      LockEntry* new_entry = array_ + added;
      new_entry->set(write->owner_lock_id_, write->owner_id_address_, kWriteLock, kNoLock);
    }
    last_active_entry_ = added;
  } else {
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
  }

  // Adjusting last_locked_entry_ is not impossible.. but let's just recalculate. Not too often.
  last_locked_entry_ = calculate_last_locked_entry();
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
  last_locked_entry_ = kLockListPositionInvalid;
  assert_sorted();
}

void CurrentLockList::release_all_after_debuglog(
  uint32_t released_read_locks,
  uint32_t released_write_locks,
  uint32_t already_released_locks,
  uint32_t canceled_async_read_locks,
  uint32_t canceled_async_write_locks) const {
  DVLOG(1) << " Unlocked " << released_read_locks << " read locks and"
    << " " << released_write_locks << " write locks. " << already_released_locks
    << " Also cancelled " << canceled_async_read_locks << " async-waiting read locks, "
    << " " << canceled_async_write_locks << " async-waiting write locks. "
    << " " << already_released_locks << " were already unlocked";
}

void CurrentLockList::giveup_all_after_debuglog(
  uint32_t givenup_read_locks,
  uint32_t givenup_write_locks,
  uint32_t givenup_upgrades,
  uint32_t already_enough_locks,
  uint32_t canceled_async_read_locks,
  uint32_t canceled_async_write_locks) const {
  DVLOG(1) << " Gave up " << givenup_read_locks << " read locks and"
    << " " << givenup_write_locks << " write locks, " << givenup_upgrades << " upgrades."
    << " Also cancelled " << canceled_async_read_locks << " async-waiting read locks, "
    << " " << canceled_async_write_locks << " async-waiting write locks. "
    << " " << already_enough_locks << " already had enough lock mode";
}

}  // namespace xct
}  // namespace foedus
