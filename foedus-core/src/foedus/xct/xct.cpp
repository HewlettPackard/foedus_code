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
  local_work_memory_ = nullptr;
  local_work_memory_size_ = 0;
  local_work_memory_cur_ = 0;
}

void Xct::initialize(memory::NumaCoreMemory* core_memory, uint32_t* mcs_block_current) {
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
  local_work_memory_ = core_memory->get_local_work_memory();
  local_work_memory_size_ = core_memory->get_local_work_memory_size();
  local_work_memory_cur_ = 0;

  current_lock_list_.init(
    core_memory->get_current_lock_list_memory(),
    core_memory->get_current_lock_list_capacity());
  retrospective_lock_list_.init(
    core_memory->get_retrospective_lock_list_memory(),
    core_memory->get_retrospective_lock_list_capacity());
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

ReadXctAccess* Xct::get_read_access(RwLockableXctId* owner_id_address) {
  ReadXctAccess* ret = NULL;
  // Return the one that's holding the S-lock (if any)
  for (uint32_t i = 0; i < read_set_size_; i++) {
    if (read_set_[i].owner_id_address_ == owner_id_address) {
      ret = read_set_ + i;
      if (ret->mcs_block_) {
        return ret;
      }
    }
  }
  return ret;
}

WriteXctAccess* Xct::get_write_access(RwLockableXctId* owner_id_address) {
  WriteXctAccess* ret = NULL;
  // Return the one that's holding the S-lock (if any)
  for (uint32_t i = 0; i < write_set_size_; i++) {
    if (write_set_[i].owner_id_address_ == owner_id_address) {
      ret = write_set_ + i;
      if (ret->locked_) {
        ASSERT_ND(ret->mcs_block_);
        return ret;
      }
    }
  }
  return ret;
}

void Xct::recover_canonical_access(thread::Thread* context, RwLockableXctId* target) {
  RwLockableXctId* new_canonical = NULL;
  for (uint32_t i = 0; i < write_set_size_; ++i) {
    auto* entry = write_set_ + i;
    if (entry->locked_) {
      if (entry->owner_id_address_ >= target) {
        ASSERT_ND(entry->mcs_block_);
        context->mcs_release_writer_lock(entry->owner_id_address_->get_key_lock(), entry->mcs_block_);
        entry->locked_ = false;
      } else {
        new_canonical = std::max(entry->owner_id_address_, new_canonical);
      }
    }
  }
  context->set_canonical_address(new_canonical);
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
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  }

  // See if I already took the S-lock
  ReadXctAccess* read = NULL;
  read = get_read_access(owner_id_address);
  if (read && read->mcs_block_) {
    return kErrorCodeOk;
  }

  // Either no need to S-lock or didn't hold an S-lock before
  read = read_set_ + read_set_size_;
  CHECK_ERROR_CODE(add_to_read_set_force(storage_id, observed_owner_id, owner_id_address));

  ASSERT_ND(read->mcs_block_ == 0);
  if (read_only && read->owner_id_address_->is_hot(context)) {
#ifdef MCS_RW_GROUP_TRY_LOCK
    if (context->mcs_try_acquire_reader_lock(
      read->owner_id_address_->get_key_lock(), &read->mcs_block_, 10)) {
      ASSERT_ND(read->mcs_block_);
      if (context->mcs_retry_acquire_reader_lock(
        read->owner_id_address_->get_key_lock(), read->mcs_block_, true)) {
        // Now we locked it, update observed xct_id; the caller, however, must make
        // sure to read the data after taking the lock, not before.
        read->observed_owner_id_ = owner_id_address->xct_id_;
        context->set_canonical_address(owner_id_address);
        return kErrorCodeOk;
      }
    }
#endif
#ifdef MCS_RW_LOCK
    if (context->mcs_try_acquire_reader_lock(
      read->owner_id_address_->get_key_lock(), &read->mcs_block_, 0)) {
      read->observed_owner_id_ = owner_id_address->xct_id_;
      context->set_canonical_address(owner_id_address);
      return kErrorCodeOk;
    }
#endif
  }
  read->mcs_block_ = 0;
  return kErrorCodeOk;
}

ErrorCode Xct::add_to_read_set_force(
  storage::StorageId storage_id,
  XctId observed_owner_id,
  RwLockableXctId* owner_id_address) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(owner_id_address);
  if (UNLIKELY(read_set_size_ >= max_read_set_size_)) {
    return kErrorCodeXctReadSetOverflow;
  }
  // The caller should set mcs_block_ after this returns.
  read_set_[read_set_size_].mcs_block_ = 0;
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
  write->mcs_block_ = 0;
  write->write_set_ordinal_ = write_set_size_;
  write->payload_address_ = payload_address;
  write->log_entry_ = log_entry;
  write->storage_id_ = storage_id;
  write->owner_id_address_ = owner_id_address;
  write->related_read_ = CXX11_NULLPTR;
  write->locked_ = false;
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

#ifndef NDEBUG
  auto* r = get_read_access(owner_id_address);
  // only S-lock reads not intended for update later
  ASSERT_ND(!(r && r->mcs_block_));
#endif
  auto* read = read_set_ + read_set_size_;
  // in this method, we force to add a read set because it's critical to confirm that
  // the physical record we write to is still the one we found.
  CHECK_ERROR_CODE(add_to_read_set_force(
    storage_id,
    observed_owner_id,
    owner_id_address));
  ASSERT_ND(read->mcs_block_ == 0);
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
