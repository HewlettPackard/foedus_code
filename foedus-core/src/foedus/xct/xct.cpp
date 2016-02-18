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
#include "foedus/xct/xct.hpp"

#include <glog/logging.h>

#include <cstring>
#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "foedus/xct/xct_options.hpp"

namespace foedus {
namespace xct {
Xct::Xct(Engine* engine, thread::ThreadId thread_id) : engine_(engine), thread_id_(thread_id) {
  id_ = XctId();
  active_ = false;
  read_set_ = nullptr;
  read_set_size_ = 0;
  max_read_set_size_ = 0;
  write_set_ = nullptr;
  write_set_size_ = 0;
  max_write_set_size_ = 0;
  lock_free_write_set_ = nullptr;
  lock_free_write_set_size_ = 0;
  max_lock_free_write_set_size_ = 0;
  pointer_set_size_ = 0;
  page_version_set_size_ = 0;
  isolation_level_ = kSerializable;
  mcs_block_current_ = nullptr;
  mcs_rw_async_mapping_current_ = nullptr;
  local_work_memory_ = nullptr;
  local_work_memory_size_ = 0;
  local_work_memory_cur_ = 0;
}

void Xct::initialize(
  memory::NumaCoreMemory* core_memory,
  uint32_t* mcs_block_current,
  uint32_t* mcs_rw_async_mapping_current) {
  id_.set_epoch(engine_->get_savepoint_manager()->get_initial_current_epoch());
  id_.set_ordinal(0);  // ordinal 0 is possible only as a dummy "latest" XctId
  ASSERT_ND(id_.is_valid());
  memory::NumaCoreMemory:: SmallThreadLocalMemoryPieces pieces
    = core_memory->get_small_thread_local_memory_pieces();
  const XctOptions& xct_opt = engine_->get_options().xct_;
  read_set_ = reinterpret_cast<ReadXctAccess*>(pieces.xct_read_access_memory_);
  read_set_size_ = 0;
  max_read_set_size_ = xct_opt.max_read_set_size_;
  write_set_ = reinterpret_cast<WriteXctAccess*>(pieces.xct_write_access_memory_);
  write_set_size_ = 0;
  max_write_set_size_ = xct_opt.max_write_set_size_;
  lock_free_write_set_ = reinterpret_cast<LockFreeWriteXctAccess*>(
    pieces.xct_lock_free_write_access_memory_);
  lock_free_write_set_size_ = 0;
  max_lock_free_write_set_size_ = xct_opt.max_lock_free_write_set_size_;
  pointer_set_ = reinterpret_cast<PointerAccess*>(pieces.xct_pointer_access_memory_);
  pointer_set_size_ = 0;
  page_version_set_ = reinterpret_cast<PageVersionAccess*>(pieces.xct_page_version_memory_);
  page_version_set_size_ = 0;
  mcs_block_current_ = mcs_block_current;
  *mcs_block_current_ = 0;
  mcs_rw_async_mapping_current_ = mcs_rw_async_mapping_current;
  *mcs_rw_async_mapping_current_ = 0;
  local_work_memory_ = core_memory->get_local_work_memory();
  local_work_memory_size_ = core_memory->get_local_work_memory_size();
  local_work_memory_cur_ = 0;

  current_lock_list_.init(
    core_memory->get_current_lock_list_memory(),
    core_memory->get_current_lock_list_capacity(),
    engine_->get_memory_manager()->get_global_volatile_page_resolver());
  retrospective_lock_list_.init(
    core_memory->get_retrospective_lock_list_memory(),
    core_memory->get_retrospective_lock_list_capacity(),
    engine_->get_memory_manager()->get_global_volatile_page_resolver());
}

void Xct::issue_next_id(XctId max_xct_id, Epoch *epoch)  {
  ASSERT_ND(id_.is_valid());

  while (true) {
    // invariant 1: Larger than latest XctId of this thread.
    XctId new_id = id_;
    // invariant 2: Larger than every XctId of any record read or written by this transaction.
    new_id.store_max(max_xct_id);
    // invariant 3: in the epoch
    if (new_id.get_epoch().before(*epoch)) {
      new_id.set_epoch(*epoch);
      new_id.set_ordinal(0);
    }
    ASSERT_ND(new_id.get_epoch() == *epoch);

    // Now, is it possible to get an ordinal one larger than this one?
    if (UNLIKELY(new_id.get_ordinal() >= kMaxXctOrdinal)) {
      // oh, that's rare.
      LOG(WARNING) << "Reached the maximum ordinal in this epoch. Advancing current epoch"
        << " just for this reason. It's rare, but not an error.";
      engine_->get_xct_manager()->advance_current_global_epoch();
      ASSERT_ND(epoch->before(engine_->get_xct_manager()->get_current_global_epoch()));
      // we have already issued fence by now, so we can use nonatomic version.
      *epoch = engine_->get_xct_manager()->get_current_global_epoch_weak();
      continue;  // try again with this epoch.
    }

    ASSERT_ND(new_id.get_ordinal() < kMaxXctOrdinal);
    new_id.set_ordinal(new_id.get_ordinal() + 1U);
    remember_previous_xct_id(new_id);
    break;
  }
}

std::ostream& operator<<(std::ostream& o, const Xct& v) {
  o << "<Xct>"
    << "<active_>" << v.is_active() << "</active_>";
  if (v.is_active()) {
    o << "<id_>" << v.get_id() << "</id_>"
      << "<read_set_size>" << v.get_read_set_size() << "</read_set_size>"
      << "<write_set_size>" << v.get_write_set_size() << "</write_set_size>"
      << "<pointer_set_size>" << v.get_pointer_set_size() << "</pointer_set_size>"
      << "<page_version_set_size>" << v.get_page_version_set_size() << "</page_version_set_size>"
      << "<lock_free_write_set_size>" << v.get_lock_free_write_set_size()
        << "</lock_free_write_set_size>";
  }
  o << "</Xct>";
  return o;
}

ErrorCode Xct::add_to_pointer_set(
  const storage::VolatilePagePointer* pointer_address,
  storage::VolatilePagePointer observed) {
  ASSERT_ND(pointer_address);
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  }

  // TASK(Hideaki) even though pointer set should be small, we don't want sequential search
  // everytime. but insertion sort requires shifting. mmm.
  for (uint32_t i = 0; i < pointer_set_size_; ++i) {
    if (pointer_set_[i].address_ == pointer_address) {
      pointer_set_[i].observed_ = observed;
      return kErrorCodeOk;
    }
  }

  if (UNLIKELY(pointer_set_size_ >= kMaxPointerSets)) {
    return kErrorCodeXctPointerSetOverflow;
  }

  // no need for fence. the observed pointer itself is the only data to verify
  pointer_set_[pointer_set_size_].address_ = pointer_address;
  pointer_set_[pointer_set_size_].observed_ = observed;
  ++pointer_set_size_;
  return kErrorCodeOk;
}

void Xct::overwrite_to_pointer_set(
  const storage::VolatilePagePointer* pointer_address,
  storage::VolatilePagePointer observed) {
  ASSERT_ND(pointer_address);
  if (isolation_level_ != kSerializable) {
    return;
  }

  for (uint32_t i = 0; i < pointer_set_size_; ++i) {
    if (pointer_set_[i].address_ == pointer_address) {
      pointer_set_[i].observed_ = observed;
      return;
    }
  }
}

ErrorCode Xct::add_to_page_version_set(
  const storage::PageVersion* version_address,
  storage::PageVersionStatus observed) {
  ASSERT_ND(version_address);
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  } else if (UNLIKELY(page_version_set_size_ >= kMaxPointerSets)) {
    return kErrorCodeXctPageVersionSetOverflow;
  }

  page_version_set_[page_version_set_size_].address_ = version_address;
  page_version_set_[page_version_set_size_].observed_ = observed;
  ++page_version_set_size_;
  return kErrorCodeOk;
}

ErrorCode Xct::add_to_read_set(
  thread::Thread* context,
  storage::StorageId storage_id,
  XctId observed_owner_id,
  RwLockableXctId* owner_id_address,
  bool read_only) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(owner_id_address);
  ASSERT_ND(observed_owner_id.is_valid());
  ASSERT_ND(!observed_owner_id.is_being_written());
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  }

  CHECK_ERROR_CODE(add_to_read_set_force(storage_id, observed_owner_id, owner_id_address));

  // We might take a pessimisitic lock for the record, which is our MOCC protocol.
  // However, we might want to do this _before_ observing XctId. Otherwise there is a
  // chance of aborts even with the lock. But then more code changes. Later, later...

  const UniversalLockId lock_id = xct_id_to_universal_lock_id(
    current_lock_list_.get_volatile_page_resolver(), owner_id_address);
  LockListPosition rll_pos = kLockListPositionInvalid;
  bool lets_take_lock = false;
  if (!retrospective_lock_list_.is_empty()) {
    // RLL is set, which means the previous run aborted for race.
    // binary-search for each read-set is not cheap, but in this case better than aborts.
    // So, let's see if we should take the lock.
    rll_pos = retrospective_lock_list_.binary_search(lock_id);
    if (rll_pos != kLockListPositionInvalid) {
      ASSERT_ND(retrospective_lock_list_.get_array()[rll_pos].universal_lock_id_ == lock_id);
      DVLOG(1) << "RLL recommends to take lock on this record!";
      lets_take_lock = true;
    }
  }

  if (!lets_take_lock && owner_id_address->is_hot(context)) {
    lets_take_lock = true;
  }

  if (lets_take_lock) {
    LockMode mode = read_only ? kReadLock : kWriteLock;
    LockListPosition cll_pos =
      current_lock_list_.get_or_add_entry(owner_id_address, mode);
    LockEntry* cll_entry = current_lock_list_.get_entry(cll_pos);
    if (cll_entry->is_enough()) {
      return kErrorCodeOk;  // already taken!
    }

    ErrorCode lock_ret;
    if (rll_pos == kLockListPositionInvalid) {
      // Then, this is a single read-lock to take.
      lock_ret = current_lock_list_.try_or_acquire_single_lock(context, cll_pos);
      // TODO(Hideaki) The above locks unconditionally in canonnical mode. Even in non-canonical,
      // when it returns kErrorCodeXctRaceAbort AND we haven't taken any write-lock yet,
      // we might still want a retry here.. but it has pros/cons. Revisit later.
    } else {
      // Then we should take all locks before this too.
      lock_ret = current_lock_list_.try_or_acquire_multiple_locks(context, cll_pos);
    }

    if (lock_ret != kErrorCodeOk) {
      ASSERT_ND(lock_ret == kErrorCodeXctRaceAbort);
      DVLOG(0) << "Failed to take some of the lock that might be beneficial later"
        << ". We still go on because the locks here are not mandatory.";
      // At this point, no point to be advised by RLL any longer.
      // Let's clear it, and let's give-up all incomplete locks in CLL.
      context->mcs_giveup_all_current_locks_after(kNullUniversalLockId);
      retrospective_lock_list_.clear_entries();
    }
  }
  return kErrorCodeOk;
}

ErrorCode Xct::add_to_read_set_force(
  storage::StorageId storage_id,
  XctId observed_owner_id,
  RwLockableXctId* owner_id_address) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(owner_id_address);
  ASSERT_ND(!observed_owner_id.is_being_written());
  if (UNLIKELY(read_set_size_ >= max_read_set_size_)) {
    return kErrorCodeXctReadSetOverflow;
  }
  // if the next-layer bit is ON, the record is not logically a record, so why we are adding
  // it to read-set? we should have already either aborted or retried in this case.
  ASSERT_ND(!observed_owner_id.is_next_layer());
  read_set_[read_set_size_].storage_id_ = storage_id;
  read_set_[read_set_size_].owner_id_address_ = owner_id_address;
  read_set_[read_set_size_].observed_owner_id_ = observed_owner_id;
  read_set_[read_set_size_].related_write_ = CXX11_NULLPTR;
  ++read_set_size_;
  return kErrorCodeOk;
}


ErrorCode Xct::add_to_write_set(
  storage::StorageId storage_id,
  RwLockableXctId* owner_id_address,
  char* payload_address,
  log::RecordLogType* log_entry) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(owner_id_address);
  ASSERT_ND(payload_address);
  ASSERT_ND(log_entry);
#ifndef NDEBUG
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG
  if (UNLIKELY(write_set_size_ >= max_write_set_size_)) {
    return kErrorCodeXctWriteSetOverflow;
  }
  WriteXctAccess* write = write_set_ + write_set_size_;
  write->write_set_ordinal_ = write_set_size_;
  write->payload_address_ = payload_address;
  write->log_entry_ = log_entry;
  write->storage_id_ = storage_id;
  uintptr_t address_ptr = reinterpret_cast<uintptr_t>(owner_id_address);
  if (address_ptr & kUniversalLockIdMsbFlag) {
    write->owner_id_address_ =
      reinterpret_cast<RwLockableXctId*>(address_ptr & ~kUniversalLockIdMsbFlag);
    write->owner_lock_id_ = to_universal_lock_id_va(owner_id_address);
  } else {
    write->owner_id_address_ = owner_id_address;
    write->owner_lock_id_ = xct_id_to_universal_lock_id(
      engine_->get_memory_manager()->get_global_volatile_page_resolver(),
      owner_id_address);
  }
  write->related_read_ = CXX11_NULLPTR;
  ++write_set_size_;
  return kErrorCodeOk;
}


ErrorCode Xct::add_to_read_and_write_set(
  storage::StorageId storage_id,
  XctId observed_owner_id,
  RwLockableXctId* owner_id_address,
  char* payload_address,
  log::RecordLogType* log_entry) {
  ASSERT_ND(observed_owner_id.is_valid());
#ifndef NDEBUG
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG
  auto* write = write_set_ + write_set_size_;
  CHECK_ERROR_CODE(add_to_write_set(storage_id, owner_id_address, payload_address, log_entry));

  auto* read = read_set_ + read_set_size_;
  // in this method, we force to add a read set because it's critical to confirm that
  // the physical record we write to is still the one we found.
  CHECK_ERROR_CODE(add_to_read_set_force(
    storage_id,
    observed_owner_id,
    owner_id_address));
  ASSERT_ND(read->owner_id_address_ == owner_id_address);
  read->related_write_ = write;
  write->related_read_ = read;
  ASSERT_ND(read->related_write_->related_read_ == read);
  ASSERT_ND(write->related_read_->related_write_ == write);
  ASSERT_ND(write->log_entry_ == log_entry);
  ASSERT_ND(write->owner_id_address_ == owner_id_address);
  ASSERT_ND(write_set_size_ > 0);
  return kErrorCodeOk;
}

ErrorCode Xct::add_to_lock_free_write_set(
    storage::StorageId storage_id,
  log::RecordLogType* log_entry) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(log_entry);
  if (UNLIKELY(lock_free_write_set_size_ >= max_lock_free_write_set_size_)) {
    return kErrorCodeXctWriteSetOverflow;
  }

#ifndef NDEBUG
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG

  lock_free_write_set_[lock_free_write_set_size_].storage_id_ = storage_id;
  lock_free_write_set_[lock_free_write_set_size_].log_entry_ = log_entry;
  ++lock_free_write_set_size_;
  return kErrorCodeOk;
}

}  // namespace xct
}  // namespace foedus
