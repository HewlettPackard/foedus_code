/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  o << "<XctId><epoch>" << v.get_epoch() << "</epoch>"
    << "<ordinal>" << v.get_ordinal() << "</ordinal>"
    << "<status>"
    << (v.is_deleted() ? "D" : " ")
    << (v.is_moved() ? "M" : " ")
    << (v.is_being_written() ? "W" : " ")
    << "</status>"
    << "</XctId>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const LockableXctId& v) {
  o << "<LockableXctId>" << v.lock_ << v.xct_id_ << "</LockableXctId>";
  return o;
}

}  // namespace xct
}  // namespace foedus
