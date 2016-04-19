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
#include "foedus/xct/sysxct_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>

#include "foedus/storage/page.hpp"
#include "foedus/thread/thread_pimpl.hpp"
#include "foedus/xct/xct_mcs_impl.hpp"

namespace foedus {
namespace xct {

////////////////////////////////////////////////////////////
/// Debugging
////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& o, const SysxctLockEntry& v) {
  o << "<SysxctLockEntry>"
    << "<LockId>" << v.universal_lock_id_ << "</LockId>"
    << "<used>" << v.used_in_this_run_ << "</used>";
  if (v.mcs_block_) {
    o << "<mcs_block_>" << v.mcs_block_ << "</mcs_block_>";
    if (v.page_lock_) {
      o << v.get_as_page_lock()->get_header();
    } else {
      o << *(v.get_as_record_lock());
    }
  } else {
    o << "<NotLocked />";
  }
  o << "</SysxctLockEntry>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const SysxctLockList& v) {
  o << "<SysxctLockList>"
    << "<Capacity>" << v.get_capacity() << "</Capacity>"
    << "<LastActiveEntry>" << v.get_last_active_entry() << "</LastActiveEntry>"
    << "<LastLockedEntry>" << v.get_last_locked_entry() << "</LastLockedEntry>"
    << "<EnclosingMaxLockId>" << v.get_enclosing_max_lock_id() << "</EnclosingMaxLockId>";
  const uint32_t kMaxShown = 32U;
  for (auto i = 1U; i <= std::min(v.last_active_entry_, kMaxShown); ++i) {
    o << std::endl << v.array_[i];
  }
  if (v.last_active_entry_ > kMaxShown) {
    o << std::endl << "<too_many />";
  }
  o << "</SysxctLockList>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const SysxctWorkspace& v) {
  o << "<SysxctWorkspace>"
    << "<running_sysxct_>" << v.running_sysxct_ << "</running_sysxct_>"
    << v.lock_list_
    << "</SysxctWorkspace>";
  return o;
}

void SysxctLockList::assert_sorted_impl() const {
  assert_last_locked_entry();
  const SysxctLockEntry* array = get_array();
  ASSERT_ND(array[kLockListPositionInvalid].universal_lock_id_ == 0);
  ASSERT_ND(array[kLockListPositionInvalid].lock_ == kNullUniversalLockId);
  ASSERT_ND(array[kLockListPositionInvalid].mcs_block_ == 0);
  const LockListPosition last_active_entry = get_last_active_entry();
  for (LockListPosition pos = 2U; pos <= last_active_entry; ++pos) {
    ASSERT_ND(array[pos - 1U].universal_lock_id_ < array[pos].universal_lock_id_);
    ASSERT_ND(array[pos].universal_lock_id_ != 0);
    ASSERT_ND(array[pos].lock_ != kNullUniversalLockId);
    const storage::Page* page = storage::to_page(reinterpret_cast<void*>(array[pos].lock_));
    auto page_id = page->get_volatile_page_id();
    ASSERT_ND(array[pos].universal_lock_id_ == to_universal_lock_id(page_id, array[pos].lock_));
  }
}

////////////////////////////////////////////////////////////
/// Data manipulation (search/add/etc)
////////////////////////////////////////////////////////////
LockListPosition SysxctLockList::lower_bound(UniversalLockId lock) const {
  assert_last_locked_entry();
  return lock_lower_bound<SysxctLockList, SysxctLockEntry>(*this, lock);
}

LockListPosition SysxctLockList::get_or_add_entry(
  storage::VolatilePagePointer page_id,
  uintptr_t lock_addr,
  bool page_lock) {
  ASSERT_ND(!is_full());
  if (UNLIKELY(is_full())) {
    LOG(FATAL) << "SysxctLockList full. This must not happen";
  }
  const UniversalLockId id = to_universal_lock_id(page_id, lock_addr);

  // Easy case? (lock >= the last entry)
  LockListPosition insert_pos = lower_bound(id);
  ASSERT_ND(insert_pos != kLockListPositionInvalid);

  // Larger than all existing entries? Append to the last!
  if (insert_pos > last_active_entry_) {
    ASSERT_ND(insert_pos == last_active_entry_ + 1U);
    LockListPosition new_pos = issue_new_position();
    array_[new_pos].set(id, lock_addr, page_lock);
    ASSERT_ND(new_pos == insert_pos);
    return new_pos;
  }

  // lower_bound returns the first entry that is NOT less than. is it equal?
  ASSERT_ND(array_[insert_pos].universal_lock_id_ >= id);
  if (array_[insert_pos].universal_lock_id_ == id) {
    // Found existing!
    return insert_pos;
  }

  DVLOG(1) << "not an easy case. We need to adjust the order. This is costly!";
  ASSERT_ND(insert_pos <= last_active_entry_);  // otherwise we went into the 1st branch
  ASSERT_ND(array_[insert_pos].universal_lock_id_ > id);  // if ==, we went into the 2nd branch
  ASSERT_ND(insert_pos == 1U || array_[insert_pos - 1U].universal_lock_id_ < id);

  LockListPosition new_last_pos = issue_new_position();
  ASSERT_ND(new_last_pos > insert_pos);
  uint64_t moved_bytes = sizeof(SysxctLockEntry) * (new_last_pos - insert_pos);
  std::memmove(array_ + insert_pos + 1U, array_ + insert_pos, moved_bytes);
  DVLOG(1) << "Re-sorted. hope this won't happen often";
  array_[insert_pos].set(id, lock_addr, page_lock);
  if (last_locked_entry_ >= insert_pos) {
    ++last_locked_entry_;
  }

  assert_sorted();
  return insert_pos;
}

LockListPosition SysxctLockList::batch_get_or_add_entries(
  storage::VolatilePagePointer page_id,
  uint32_t lock_count,
  uintptr_t* lock_addr,
  bool page_lock) {
  ASSERT_ND(lock_count);
  ASSERT_ND(get_last_active_entry() + lock_count <= kMaxSysxctLocks);
  if (UNLIKELY(get_last_active_entry() + lock_count > kMaxSysxctLocks)) {
    LOG(FATAL) << "SysxctLockList full. This must not happen";
  }
  if (lock_count == 0) {
    return kLockListPositionInvalid;
  }

  // Address order within the same page guarantees the Universal lock ID order.
  if (std::is_sorted(lock_addr, lock_addr + lock_count)) {
    DVLOG(3) << "Okay, sorted.";
  } else {
    LOG(INFO) << "Given array is not sorted. We have to sort it here. Is it intended??";
    std::sort(lock_addr, lock_addr + lock_count);
    // If this is the majority case, the is_sorted() is a wasted cost (std::sort often
    // do something like this internally), but this should almost never happen. In that
    // case, std::is_sorted() is faster.
  }

  const UniversalLockId min_id = to_universal_lock_id(page_id, lock_addr[0]);
  const UniversalLockId max_id = to_universal_lock_id(page_id, lock_addr[lock_count - 1]);
  ASSERT_ND(min_id <= max_id);

  // We optimize for the case where this list has nothing in [min_lock_addr,max_lock_addr].
  // When this is not the case, we just call get_or_add_entry with for loop.
  // A large lock_count should mean page split sysxct, so this should never happen.

  const LockListPosition insert_pos = lower_bound(min_id);
  ASSERT_ND(insert_pos != kLockListPositionInvalid);
  if (insert_pos > last_active_entry_) {
    ASSERT_ND(insert_pos == last_active_entry_ + 1U);
    // Larger than all existing entries. This is easy. We just append all to the end.
  } else {
    const UniversalLockId next_id = array_[insert_pos].universal_lock_id_;
    ASSERT_ND(next_id >= min_id);  // lower_bound()'s contract
    if (next_id > max_id) {
      DVLOG(3) << "Okay, no overlap. Much easier.";
    } else {
      // Most likely we hit this case for page locks (just 2 or 3 locks)
      // Otherwise something wrong...
      if (UNLIKELY(lock_count > 5U)) {
        LOG(INFO) << "Really? Overlapped entry for a large batch. This is quite unexpected!";
      }

      // Fall back to the slow path
      for (uint32_t i = 0; i < lock_count; ++i) {
        LockListPosition pos = get_or_add_entry(page_id, lock_addr[i], page_lock);
        ASSERT_ND(pos >= insert_pos);
      }
      return insert_pos;
    }
  }

  ASSERT_ND(insert_pos > last_active_entry_ || array_[insert_pos].universal_lock_id_ >= max_id);

  // Move existing entries at once, then set() in a tight for loop
  ASSERT_ND(last_active_entry_ + 1U >= insert_pos);
  const uint32_t move_count = last_active_entry_ + 1U - insert_pos;
  if (move_count > 0) {
    const uint64_t moved_bytes = sizeof(SysxctLockEntry) * move_count;
    std::memmove(array_ + insert_pos + lock_count, array_ + insert_pos, moved_bytes);
  }
  if (last_locked_entry_ >= insert_pos) {
    last_locked_entry_ += lock_count;
  }

  for (uint32_t i = 0; i < lock_count; ++i) {
    const UniversalLockId id = to_universal_lock_id(page_id, lock_addr[i]);
    array_[insert_pos + i].set(id, lock_addr[i], page_lock);
  }

  last_active_entry_ += lock_count;
  assert_sorted();
  return insert_pos;
}

void SysxctLockList::compress_entries(UniversalLockId enclosing_max_lock_id) {
  assert_sorted();
  ASSERT_ND(last_locked_entry_ == kLockListPositionInvalid);
  LockListPosition next_compressed_pos = 1U;
  for (LockListPosition pos = 1U; pos <= last_active_entry_; ++pos) {
    ASSERT_ND(!array_[pos].is_locked());
    ASSERT_ND(pos >= next_compressed_pos);
    if (array_[pos].used_in_this_run_) {
      // Okay, let's inherit this entry
      if (pos != next_compressed_pos) {
        array_[next_compressed_pos] = array_[pos];
      }
      array_[next_compressed_pos].used_in_this_run_ = false;
      ++next_compressed_pos;
    }
  }
  last_active_entry_ = next_compressed_pos - 1U;
  enclosing_max_lock_id_ = enclosing_max_lock_id;
  assert_sorted();
}


}  // namespace xct
}  // namespace foedus
