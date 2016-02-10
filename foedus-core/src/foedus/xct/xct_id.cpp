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
#include "foedus/xct/xct_id.hpp"

#include <ostream>

#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pimpl.hpp"

namespace foedus {
namespace xct {

bool RwLockableXctId::is_hot(thread::Thread* context) const {
  return foedus::storage::to_page(this)->get_header().contains_hot_records(context);
}

void RwLockableXctId::hotter(thread::Thread* context) const {
  foedus::storage::to_page(this)->get_header().hotness_.increment(&context->get_lock_rnd());
}

McsBlockIndex McsLock::acquire_lock(thread::Thread* context) {
  // not inlined. so, prefer calling it directly
  return context->mcs_acquire_lock(this);
}
McsBlockIndex McsLock::initial_lock(thread::Thread* context) {
  return context->mcs_initial_lock(this);
}
void McsLock::release_lock(thread::Thread* context, McsBlockIndex block) {
  // not inlined. so, prefer calling it directly
  context->mcs_release_lock(this, block);
}

void McsLock::ownerless_acquire_lock() {
  thread::Thread::mcs_ownerless_acquire_lock(this);
}

void McsLock::ownerless_release_lock() {
  thread::Thread::mcs_ownerless_release_lock(this);
}

void McsLock::ownerless_initial_lock() {
  thread::Thread::mcs_ownerless_initial_lock(this);
}

McsRwLockScope::McsRwLockScope(bool as_reader)
  : context_(nullptr), lock_(nullptr), block_(0), as_reader_(as_reader) {}

McsRwLockScope::McsRwLockScope(
  thread::Thread* context,
  RwLockableXctId* lock,
  bool as_reader,
  bool acquire_now,
  bool is_try_acquire)
  : context_(context), lock_(lock->get_key_lock()), block_(0), as_reader_(as_reader) {
  if (acquire_now) {
    acquire_general(is_try_acquire);
  }
}

McsRwLockScope::McsRwLockScope(
  thread::Thread* context,
  McsRwLock* lock,
  bool as_reader,
  bool acquire_now,
  bool is_try_acquire)
  : context_(context), lock_(lock), block_(0), as_reader_(as_reader) {
  if (acquire_now) {
    acquire_general(is_try_acquire);
  }
}

bool McsRwLockScope::acquire_general(bool is_try_acquire) {
  if (is_try_acquire) {
    return try_acquire();
  } else {
    unconditional_acquire();
    return true;
  }
}
void McsRwLockScope::initialize(
  thread::Thread* context,
  McsRwLock* lock,
  bool as_reader,
  bool acquire_now,
  bool is_try_acquire) {
  if (is_valid() && is_locked()) {
    release();
  }
  context_ = context;
  lock_ = lock;
  block_ = 0;
  as_reader_ = as_reader;
  if (acquire_now) {
    acquire_general(is_try_acquire);
  }
}

McsRwLockScope::~McsRwLockScope() {
  release();
  context_ = nullptr;
  lock_ = nullptr;
}

McsRwLockScope::McsRwLockScope(McsRwLockScope&& other) {
  context_ = other.context_;
  lock_ = other.lock_;
  block_ = other.block_;
  as_reader_ = other.as_reader_;
  other.block_ = 0;
}

McsRwLockScope& McsRwLockScope::operator=(McsRwLockScope&& other) {
  if (is_valid()) {
    release();
  }
  context_ = other.context_;
  lock_ = other.lock_;
  block_ = other.block_;
  as_reader_ = other.as_reader_;
  other.block_ = 0;
  return *this;
}

void McsRwLockScope::unconditional_acquire() {
  if (is_valid()) {
    if (block_ == 0) {
      if (as_reader_) {
        block_ = context_->mcs_acquire_reader_lock(lock_);
      } else {
        block_ = context_->mcs_acquire_writer_lock(lock_);
      }
      ASSERT_ND(block_);
    }
  }
}
bool McsRwLockScope::try_acquire() {
  if (is_valid()) {
    if (block_ == 0) {
      if (as_reader_) {
        block_ = context_->mcs_try_acquire_reader_lock(lock_);
      } else {
        block_ = context_->mcs_try_acquire_writer_lock(lock_);
      }
    }
    return block_ != 0;
  } else {
    return false;
  }
}

void McsRwLockScope::release() {
  if (is_valid()) {
    if (block_) {
      if (as_reader_) {
        context_->mcs_release_reader_lock(lock_, block_);
      } else {
        context_->mcs_release_writer_lock(lock_, block_);
      }
      block_ = 0;
    }
  }
}

McsLockScope::McsLockScope() : context_(nullptr), lock_(nullptr), block_(0) {}

McsLockScope::McsLockScope(
  thread::Thread* context,
  LockableXctId* lock,
  bool acquire_now,
  bool non_racy_acquire)
  : context_(context), lock_(lock->get_key_lock()), block_(0) {
  if (acquire_now) {
    acquire(non_racy_acquire);
  }
}

McsLockScope::McsLockScope(
  thread::Thread* context,
  McsLock* lock,
  bool acquire_now,
  bool non_racy_acquire)
  : context_(context), lock_(lock), block_(0) {
  if (acquire_now) {
    acquire(non_racy_acquire);
  }
}

void McsLockScope::initialize(
  thread::Thread* context,
  McsLock* lock,
  bool acquire_now,
  bool non_racy_acquire) {
  if (is_valid() && is_locked()) {
    release();
  }
  context_ = context;
  lock_ = lock;
  block_ = 0;
  if (acquire_now) {
    acquire(non_racy_acquire);
  }
}

McsLockScope::~McsLockScope() {
  release();
  context_ = nullptr;
  lock_ = nullptr;
}

McsLockScope::McsLockScope(McsLockScope&& other) {
  context_ = other.context_;
  lock_ = other.lock_;
  block_ = other.block_;
  other.block_ = 0;
}

McsLockScope& McsLockScope::operator=(McsLockScope&& other) {
  if (is_valid()) {
    release();
  }
  context_ = other.context_;
  lock_ = other.lock_;
  block_ = other.block_;
  other.block_ = 0;
  return *this;
}

void McsLockScope::move_to(storage::PageVersionLockScope* new_owner) {
  ASSERT_ND(is_locked());
  new_owner->context_ = context_;
  // PageVersion's first member is McsLock, so this is ok.
  new_owner->version_ = reinterpret_cast<storage::PageVersion*>(lock_);
  ASSERT_ND(lock_ == &new_owner->version_->lock_);
  new_owner->block_ = block_;
  new_owner->changed_ = false;
  new_owner->released_ = false;
  context_ = nullptr;
  lock_ = nullptr;
  block_ = 0;
  ASSERT_ND(!is_locked());
}

void McsLockScope::acquire(bool non_racy_acquire) {
  if (is_valid()) {
    if (block_ == 0) {
      if (non_racy_acquire) {
        block_ = context_->mcs_initial_lock(lock_);
      } else {
        block_ = context_->mcs_acquire_lock(lock_);
      }
    }
  }
}

void McsLockScope::release() {
  if (is_valid()) {
    if (block_) {
      context_->mcs_release_lock(lock_, block_);
      block_ = 0;
    }
  }
}

McsOwnerlessLockScope::McsOwnerlessLockScope() : lock_(nullptr), locked_by_me_(false) {}
McsOwnerlessLockScope::McsOwnerlessLockScope(
  McsLock* lock,
  bool acquire_now,
  bool non_racy_acquire)
  : lock_(lock), locked_by_me_(false) {
  if (acquire_now) {
    acquire(non_racy_acquire);
  }
}
McsOwnerlessLockScope::~McsOwnerlessLockScope() {
  release();
  lock_ = nullptr;
  locked_by_me_ = false;
}

void McsOwnerlessLockScope::acquire(bool non_racy_acquire) {
  if (is_valid()) {
    if (!is_locked_by_me()) {
      if (non_racy_acquire) {
        lock_->ownerless_initial_lock();
      } else {
        lock_->ownerless_acquire_lock();
      }
      locked_by_me_ = true;
    }
  }
}

void McsOwnerlessLockScope::release() {
  if (is_valid()) {
    if (is_locked_by_me()) {
      lock_->ownerless_release_lock();
      locked_by_me_ = false;
    }
  }
}

std::ostream& operator<<(std::ostream& o, const McsLock& v) {
  o << "<McsLock><locked>" << v.is_locked() << "</locked><tail_waiter>"
    << v.get_tail_waiter() << "</tail_waiter><tail_block>" << v.get_tail_waiter_block()
    << "</tail_block></McsLock>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const XctId& v) {
  o << "<XctId epoch=\"" << v.get_epoch()
    << "\" ordinal=\"" << v.get_ordinal()
    << "\" status=\""
      << (v.is_deleted() ? "D" : " ")
      << (v.is_moved() ? "M" : " ")
      << (v.is_being_written() ? "W" : " ")
      << (v.is_next_layer() ? "N" : " ")
    << "\" />";
  return o;
}

std::ostream& operator<<(std::ostream& o, const LockableXctId& v) {
  o << "<LockableXctId>" << v.xct_id_ << v.lock_ << "</LockableXctId>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const McsRwLock& v) {
  o << "<McsRwLock><locked>" << v.is_locked() << "</locked><tail_waiter>"
    << v.get_tail_waiter() << "</tail_waiter><tail_block>" << v.get_tail_waiter_block()
    << "</tail_block></McsRwLock>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const RwLockableXctId& v) {
  o << "<RwLockableXctId>" << v.xct_id_ << v.lock_ << "</RwLockableXctId>";
  return o;
}
}  // namespace xct
}  // namespace foedus
