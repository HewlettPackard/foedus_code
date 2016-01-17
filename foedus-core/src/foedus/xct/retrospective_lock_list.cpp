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

void RetrospectiveLockList::init(RetrospectiveLock* array, uint32_t capacity) {
  array_ = array;
  capacity_ = capacity;
  clear_entries();
}

void RetrospectiveLockList::clear_entries() {
  last_active_entry_ = kLockListPositionInvalid;
  last_canonically_locked_entry_ = kLockListPositionInvalid;
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

void CurrentLockList::init(CurrentLock* array, uint32_t capacity) {
  array_ = array;
  capacity_ = capacity;
  clear_entries();
}

void CurrentLockList::clear_entries() {
  last_active_entry_ = kLockListPositionInvalid;
  in_canonical_mode_ = true;
  if (array_) {
    // Dummy entry
    array_[kLockListPositionInvalid].universal_lock_id_ = 0;
    array_[kLockListPositionInvalid].lock_ = nullptr;
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
std::ostream& operator<<(std::ostream& o, const CurrentLock& v) {
  o << "<CurrentLock>"
    << "<LockId>" << v.universal_lock_id_ << "</LockId>"
    << "<TakenMode>" << v.taken_mode_ << "</TakenMode>"
    << *(v.lock_)
    << "</CurrentLock>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const CurrentLockList& v) {
  o << "<CurrentLockList>"
    << "<Capacity>" << v.capacity_ << "</Capacity>"
    << "<InCanonicalMode>" << v.in_canonical_mode_ << "</InCanonicalMode>"
    << "<LastActiveEntry>" << v.last_active_entry_ << "</LastActiveEntry>";
  const uint32_t kMaxShown = 32U;
  for (auto i = 1U; i <= std::min(v.last_active_entry_, kMaxShown); ++i) {
    o << v.array_[i];
  }
  o << "</CurrentLockList>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const RetrospectiveLock& v) {
  o << "<RetrospectiveLock>"
    << "<LockId>" << v.universal_lock_id_ << "</LockId>"
    << "<PreferredMode>" << v.preferred_mode_ << "</PreferredMode>"
    << "<TakenMode>" << v.taken_mode_ << "</TakenMode>"
    << *(v.lock_)
    << "</RetrospectiveLock>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const RetrospectiveLockList& v) {
  o << "<RetrospectiveLockList>"
    << "<Capacity>" << v.capacity_ << "</Capacity>"
    << "<LastActiveEntry>" << v.last_active_entry_ << "</LastActiveEntry>"
    << "<LastCanonicallyLockedEntry>"
      << v.last_canonically_locked_entry_ << "</LastCanonicallyLockedEntry>";
  const uint32_t kMaxShown = 32U;
  for (auto i = 1U; i <= std::min(v.last_active_entry_, kMaxShown); ++i) {
    o << v.array_[i];
  }
  o << "</RetrospectiveLockList>";
  return o;
}


template<typename LOCK_LIST>
void lock_assert_sorted(const LOCK_LIST& list) {
  const typename LOCK_LIST::EntryType* array = list.get_array();
  ASSERT_ND(array[kLockListPositionInvalid].universal_lock_id_ == 0);
  ASSERT_ND(array[kLockListPositionInvalid].lock_ == nullptr);
  ASSERT_ND(array[kLockListPositionInvalid].taken_mode_ == kNoLock);
  const LockListPosition last_active_entry = list.get_last_active_entry();
  for (LockListPosition pos = 2U; pos <= last_active_entry; ++pos) {
    ASSERT_ND(array[pos - 1U].universal_lock_id_ < array[pos].universal_lock_id_);
    ASSERT_ND(array[pos].universal_lock_id_ != 0);
    ASSERT_ND(array[pos].lock_ != nullptr);
  }
}

void CurrentLockList::assert_sorted_impl() const {
  lock_assert_sorted(*this);
}
void RetrospectiveLockList::assert_sorted_impl() const {
  lock_assert_sorted(*this);
}

////////////////////////////////////////////////////////////
/// Data manipulation (search/add/etc)
/// These implementations are skewed towards sorted cases,
/// meaning it runs faster when accesses are nicely sorted.
////////////////////////////////////////////////////////////
LockListPosition CurrentLockList::add_entry(RwLockableXctId* lock, LockMode taken_mode) {
  ASSERT_ND(taken_mode != kNoLock);
  // Easy case? (lock >= the last entry)
  const UniversalLockId id = to_universal_lock_id(lock);
  if (last_active_entry_ == kLockListPositionInvalid) {
    LockListPosition new_pos = issue_new_position();
    array_[new_pos].set(id, lock, taken_mode);
    return new_pos;
  }

  ASSERT_ND(last_active_entry_ != kLockListPositionInvalid);
  if (array_[last_active_entry_].universal_lock_id_ <= id) {
    if (array_[last_active_entry_].universal_lock_id_ == id) {
      // Overwriting the existing, last entry. It must be read-lock -> write-lock upgrade.
      ASSERT_ND(array_[last_active_entry_].taken_mode_ == kReadLock);
      ASSERT_ND(taken_mode == kWriteLock);
      array_[last_active_entry_].taken_mode_ = taken_mode;
      return last_active_entry_;
    }

    ASSERT_ND(array_[last_active_entry_].universal_lock_id_ < id);
    LockListPosition new_pos = issue_new_position();
    array_[new_pos].set(id, lock, taken_mode);
    return new_pos;
  }

  DVLOG(1) << "not an easy case. We need to adjust the order. This is costly!";
  LockListPosition insert_pos =
    std::lower_bound(
      array_ + 1U,
      array_ + last_active_entry_ + 1U,
      id,
      CurrentLockLessThan())
    - array_;
  ASSERT_ND(insert_pos != kLockListPositionInvalid);
  ASSERT_ND(insert_pos < last_active_entry_);  // otherwise we went into the branch above
  ASSERT_ND(array_[insert_pos].universal_lock_id_ >= id);
  ASSERT_ND(insert_pos == 1U || array_[insert_pos - 1U].universal_lock_id_ < id);
  if (array_[insert_pos].universal_lock_id_ == id) {
    // Overwriting the existing. lucky. It must be read-lock -> write-lock upgrade.
    ASSERT_ND(array_[insert_pos].taken_mode_ == kReadLock);
    ASSERT_ND(taken_mode == kWriteLock);
    array_[insert_pos].taken_mode_ = taken_mode;
    return insert_pos;
  }

  LockListPosition new_last_pos = issue_new_position();
  uint64_t moved_bytes = sizeof(CurrentLock) * (new_last_pos - insert_pos);
  std::memmove(array_ + insert_pos, array_ + new_last_pos, moved_bytes);
  DVLOG(1) << "Re-sorted. hope this won't happen often";
  array_[insert_pos].set(id, lock, taken_mode);
  return insert_pos;
}


template<typename LOCK_LIST, typename LOCK_LESSTHAN>
LockListPosition lock_binary_search(const LOCK_LIST& list, RwLockableXctId* lock) {
  const UniversalLockId id = to_universal_lock_id(lock);
  LockListPosition last_active_entry = list.get_last_active_entry();
  if (last_active_entry == kLockListPositionInvalid) {
    return kLockListPositionInvalid;
  }
  // Check the easy cases first. This will be an wasted cost if it's not, but still cheap.
  const typename LOCK_LIST::EntryType* array = list.get_array();
  if (array[last_active_entry].universal_lock_id_ <= id) {
    if (array[last_active_entry].universal_lock_id_ == id) {
      return last_active_entry;
    }

    return kLockListPositionInvalid;
  }
  DVLOG(1) << "not an easy case. Binary search!";
  LockListPosition pos
    = std::lower_bound(
        array + 1U,
        array + last_active_entry + 1U,
        id,
        LOCK_LESSTHAN())
      - array;
  ASSERT_ND(pos != kLockListPositionInvalid);
  ASSERT_ND(pos < last_active_entry);  // otherwise we went into the branch above
  ASSERT_ND(array[pos].universal_lock_id_ >= id);
  ASSERT_ND(pos == 1U || array[pos - 1U].universal_lock_id_ < id);

  if (array[pos].universal_lock_id_ == id) {
    return pos;
  } else {
    return kLockListPositionInvalid;
  }
}

LockListPosition CurrentLockList::binary_search(RwLockableXctId* lock) const {
  return lock_binary_search<CurrentLockList, CurrentLockLessThan>(*this, lock);
}

LockListPosition RetrospectiveLockList::binary_search(RwLockableXctId* lock) const {
  return lock_binary_search<RetrospectiveLockList, RetrospectiveLockLessThan>(*this, lock);
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

  last_canonically_locked_entry_ = kLockListPositionInvalid;
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
    array_[pos].set(to_universal_lock_id(lock), lock, kReadLock, kNoLock);
  }
  DVLOG(1) << "Added " << last_active_entry_ << " to RLL for read-locks";

  // Writes are always added to RLL.
  for (uint32_t i = 0; i < write_set_size; ++i) {
    RwLockableXctId* lock = write_set[i].owner_id_address_;
    auto pos = issue_new_position();
    array_[pos].set(to_universal_lock_id(lock), lock, kWriteLock, kNoLock);
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
        std::memcpy(array_ + prev_pos + 1U, array_ + pos, sizeof(RetrospectiveLock));
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


}  // namespace xct
}  // namespace foedus
