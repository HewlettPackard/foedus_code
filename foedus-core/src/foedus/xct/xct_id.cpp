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

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/assorted/spin_until_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pimpl.hpp"

namespace foedus {
namespace xct {

UniversalLockId to_universal_lock_id(
  const memory::GlobalVolatilePageResolver& resolver,
  uintptr_t lock_ptr) {
  storage::assert_within_valid_volatile_page(resolver, reinterpret_cast<void*>(lock_ptr));
  const storage::Page* page = storage::to_page(reinterpret_cast<void*>(lock_ptr));
  const auto& page_header = page->get_header();
  ASSERT_ND(!page_header.snapshot_);
  storage::VolatilePagePointer vpp(storage::construct_volatile_page_pointer(page_header.page_id_));
  const uint64_t node = vpp.get_numa_node();
  const uint64_t page_index = vpp.get_offset();

  // See assert_within_valid_volatile_page() why we can't do these assertions.
  // ASSERT_ND(lock_ptr >= base + vpp.components.offset * storage::kPageSize);
  // ASSERT_ND(lock_ptr < base + (vpp.components.offset + 1U) * storage::kPageSize);

  // Although we have the addresses in resolver, we can NOT use it to calculate the offset
  // because the base might be a different VA (though pointing to the same physical address).
  // We thus calculate UniversalLockId purely from PageId in the page header and in_page_offset.
  // Thus, actually this function uses resolver only for assertions (so far)!
  ASSERT_ND(node < resolver.numa_node_count_);
  ASSERT_ND(vpp.get_offset() >= resolver.begin_);
  ASSERT_ND(vpp.get_offset() < resolver.end_);
  return to_universal_lock_id(node, page_index, lock_ptr);
}

RwLockableXctId* from_universal_lock_id(
  const memory::GlobalVolatilePageResolver& resolver,
  UniversalLockId universal_lock_id) {
  uint16_t node = universal_lock_id >> 48;
  uint64_t offset = universal_lock_id & ((1ULL << 48) - 1ULL);
  uintptr_t base = reinterpret_cast<uintptr_t>(resolver.bases_[node]);
  return reinterpret_cast<RwLockableXctId*>(base + offset);
}

bool RwLockableXctId::is_hot(thread::Thread* context) const {
  return foedus::storage::to_page(this)->get_header().contains_hot_records(context);
}

void RwLockableXctId::hotter(thread::Thread* context) const {
  foedus::storage::to_page(this)->get_header().hotness_.increment(&context->get_lock_rnd());
}

McsBlockIndex McsWwLock::acquire_lock(thread::Thread* context) {
  // not inlined. so, prefer calling it directly
  return context->mcs_acquire_lock(this);
}
McsBlockIndex McsWwLock::initial_lock(thread::Thread* context) {
  return context->mcs_initial_lock(this);
}
void McsWwLock::release_lock(thread::Thread* context, McsBlockIndex block) {
  // not inlined. so, prefer calling it directly
  context->mcs_release_lock(this, block);
}

void McsWwLock::ownerless_acquire_lock() {
  thread::Thread::mcs_ownerless_acquire_lock(this);
}

void McsWwLock::ownerless_release_lock() {
  thread::Thread::mcs_ownerless_release_lock(this);
}

void McsWwLock::ownerless_initial_lock() {
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

McsWwLockScope::McsWwLockScope() : context_(nullptr), lock_(nullptr), block_(0) {}

McsWwLockScope::McsWwLockScope(
  thread::Thread* context,
  LockableXctId* lock,
  bool acquire_now,
  bool non_racy_acquire)
  : context_(context), lock_(lock->get_key_lock()), block_(0) {
  if (acquire_now) {
    acquire(non_racy_acquire);
  }
}

McsWwLockScope::McsWwLockScope(
  thread::Thread* context,
  McsWwLock* lock,
  bool acquire_now,
  bool non_racy_acquire)
  : context_(context), lock_(lock), block_(0) {
  if (acquire_now) {
    acquire(non_racy_acquire);
  }
}

void McsWwLockScope::initialize(
  thread::Thread* context,
  McsWwLock* lock,
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

McsWwLockScope::~McsWwLockScope() {
  release();
  context_ = nullptr;
  lock_ = nullptr;
}

McsWwLockScope::McsWwLockScope(McsWwLockScope&& other) {
  context_ = other.context_;
  lock_ = other.lock_;
  block_ = other.block_;
  other.block_ = 0;
}

McsWwLockScope& McsWwLockScope::operator=(McsWwLockScope&& other) {
  if (is_valid()) {
    release();
  }
  context_ = other.context_;
  lock_ = other.lock_;
  block_ = other.block_;
  other.block_ = 0;
  return *this;
}

void McsWwLockScope::acquire(bool non_racy_acquire) {
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

void McsWwLockScope::release() {
  if (is_valid()) {
    if (block_) {
      context_->mcs_release_lock(lock_, block_);
      block_ = 0;
    }
  }
}

McsOwnerlessLockScope::McsOwnerlessLockScope() : lock_(nullptr), locked_by_me_(false) {}
McsOwnerlessLockScope::McsOwnerlessLockScope(
  McsWwLock* lock,
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

/////////////////////////////////////////////////////////////////
///
///    MCS block classes
///
/////////////////////////////////////////////////////////////////
bool McsRwSimpleBlock::timeout_granted(int32_t timeout) {
  if (timeout == kTimeoutNever) {
    assorted::spin_until([this]{ return this->is_granted(); });
    return true;
  } else {
    while (--timeout) {
      if (is_granted()) {
        return true;
      }
      assorted::yield_if_valgrind();
    }
    return is_granted();
  }
}

bool McsRwExtendedBlock::timeout_granted(int32_t timeout) {
  if (timeout == kTimeoutZero) {
    return pred_flag_is_granted();
  } else if (timeout == kTimeoutNever) {
    assorted::spin_until([this]{ return this->pred_flag_is_granted(); });
    ASSERT_ND(pred_flag_is_granted());
  } else {
    int32_t cycles = 0;
    do {
      if (pred_flag_is_granted()) {
        return true;
      }
      assorted::yield_if_valgrind();
    } while (++cycles < timeout);
  }
  return pred_flag_is_granted();
}

/////////////////////////////////////////////////////////////////
///
/// Debug out operators
///
/////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& o, const McsWwLock& v) {
  o << "<McsWwLock><locked>" << v.is_locked() << "</locked><tail_waiter>"
    << v.get_tail_waiter() << "</tail_waiter><tail_block>" << v.get_tail_waiter_block()
    << "</tail_block></McsWwLock>";
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

static_assert(storage::kPageSize == kLockPageSize, "kLockPageSize incorrect");

}  // namespace xct
}  // namespace foedus
