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

#include "foedus/thread/thread.hpp"

namespace foedus {
namespace xct {

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

McsLockScope::McsLockScope(thread::Thread* context, LockableXctId* lock) {
  context_ = context;
  lock_ = lock->get_key_lock();
  block_ = context->mcs_acquire_lock(lock_);
}

McsLockScope::McsLockScope(thread::Thread* context, McsLock* lock) {
  context_ = context;
  lock_ = lock;
  block_ = context->mcs_acquire_lock(lock_);
}

McsLockScope::~McsLockScope() {
  ASSERT_ND(block_);
  context_->mcs_release_lock(lock_, block_);
}


std::ostream& operator<<(std::ostream& o, const McsLock& v) {
  o << "<McsLock><locked>" << v.is_locked() << "</locked><tail_waiter>"
    << v.get_tail_waiter() << "</tail_waiter><tail_block>" << v.get_tail_waiter_block()
    << "</tail_block></McsLock>";
  return o;
}
std::ostream& operator<<(std::ostream& o, const CombinedLock& v) {
  o << "<CombinedLock>" << *v.get_key_lock()
    << "<other_lock>"
      << (v.is_rangelocked() ? "R" : " ")
    << "</other_lock></CombinedLock>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const XctId& v) {
  o << "<XctId epoch=\"" << v.get_epoch()
    << "\" ordinal=\"" << v.get_ordinal()
    << "\" status=\""
      << (v.is_deleted() ? "D" : " ")
      << (v.is_moved() ? "M" : " ")
      << (v.is_being_written() ? "W" : " ")
    << "\" />";
  return o;
}

std::ostream& operator<<(std::ostream& o, const LockableXctId& v) {
  o << "<LockableXctId>" << v.xct_id_ << v.lock_ << "</LockableXctId>";
  return o;
}

}  // namespace xct
}  // namespace foedus
