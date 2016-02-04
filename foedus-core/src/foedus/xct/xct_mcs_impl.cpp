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
#include "foedus/xct/xct_mcs_impl.hpp"

#include <glog/logging.h>

#include <atomic>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "foedus/xct/xct_mcs_adapter_impl.hpp"

namespace foedus {
namespace xct {

inline void assert_mcs_aligned(const void* address) {
  ASSERT_ND(address);
  ASSERT_ND(reinterpret_cast<uintptr_t>(address) % 4 == 0);
}

/** Spin locally until the given condition returns false */
template <typename COND>
void spin_until(COND spin_while_cond) {
  DVLOG(1) << "Locally spinning...";
  uint64_t spins = 0;
  while (spin_while_cond()) {
    ++spins;
    if ((spins & 0xFFFFFFU) == 0) {
      assorted::spinlock_yield();
    }
  }
  DVLOG(1) << "Spin ended. Spent " << spins << " spins";
}

////////////////////////////////////////////////////////////////////////////////
///
///      WW-lock implementations (all simple versions)
///
////////////////////////////////////////////////////////////////////////////////
template <typename ADAPTOR>
McsBlockIndex McsImpl<ADAPTOR>::acquire_unconditional_ww(McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient mfences (note, xchg implies lock prefix. not a compiler's bug!).
  ASSERT_ND(!adaptor_.me_waiting()->load());
  assert_mcs_aligned(mcs_lock);
  // so far we allow only 2^16 MCS blocks per transaction. we might increase later.
  ASSERT_ND(adaptor_.get_cur_block() < 0xFFFFU);
  McsBlockIndex block_index = adaptor_.issue_new_block();
  ASSERT_ND(block_index > 0);
  ASSERT_ND(block_index <= 0xFFFFU);
  McsBlock* my_block = adaptor_.get_ww_my_block(block_index);
  my_block->clear_successor_release();
  adaptor_.me_waiting()->store(true, std::memory_order_release);
  const thread::ThreadId id = adaptor_.get_my_id();
  uint32_t desired = McsLock::to_int(id, block_index);
  uint32_t group_tail = desired;
  uint32_t* address = &(mcs_lock->data_);
  assert_mcs_aligned(address);

  uint32_t pred_int = 0;
  while (true) {
    // if it's obviously locked by a guest, we should wait until it's released.
    // so far this is busy-wait, we can do sth. to prevent priority inversion later.
    if (UNLIKELY(*address == kMcsGuestId)) {
      spin_until([address]{
        return assorted::atomic_load_acquire<uint32_t>(address) == kMcsGuestId;
      });
    }

    // atomic op should imply full barrier, but make sure announcing the initialized new block.
    ASSERT_ND(group_tail != kMcsGuestId);
    ASSERT_ND(group_tail != 0);
    ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != group_tail);
    pred_int = assorted::raw_atomic_exchange<uint32_t>(address, group_tail);
    ASSERT_ND(pred_int != group_tail);
    ASSERT_ND(pred_int != desired);

    if (pred_int == 0) {
      // this means it was not locked.
      ASSERT_ND(mcs_lock->is_locked());
      DVLOG(2) << "Okay, got a lock uncontended. me=" << id;
      adaptor_.me_waiting()->store(false, std::memory_order_release);
      ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
      return block_index;
    } else if (UNLIKELY(pred_int == kMcsGuestId)) {
      // ouch, I don't want to keep the guest ID! return it back.
      // This also determines the group_tail of this queue
      group_tail = assorted::raw_atomic_exchange<uint32_t>(address, kMcsGuestId);
      ASSERT_ND(group_tail != 0 && group_tail != kMcsGuestId);
      continue;
    } else {
      break;
    }
  }

  ASSERT_ND(pred_int != 0 && pred_int != kMcsGuestId);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != kMcsGuestId);
  McsLock old;
  old.data_ = pred_int;
  ASSERT_ND(mcs_lock->is_locked());
  thread::ThreadId predecessor_id = old.get_tail_waiter();
  ASSERT_ND(predecessor_id != id);
  McsBlockIndex predecessor_block = old.get_tail_waiter_block();
  DVLOG(0) << "mm, contended, we have to wait.. me=" << id << " pred=" << predecessor_id;

  ASSERT_ND(adaptor_.me_waiting()->load());
  ASSERT_ND(adaptor_.get_other_cur_block(predecessor_id) >= predecessor_block);
  McsBlock* pred_block = adaptor_.get_ww_other_block(predecessor_id, predecessor_block);
  ASSERT_ND(!pred_block->has_successor());

  pred_block->set_successor_release(id, block_index);

  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != kMcsGuestId);
  spin_until([this]{ return this->adaptor_.me_waiting()->load(std::memory_order_acquire); });
  DVLOG(1) << "Okay, now I hold the lock. me=" << id << ", ex-pred=" << predecessor_id;
  ASSERT_ND(!adaptor_.me_waiting()->load());
  ASSERT_ND(mcs_lock->is_locked());
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != 0);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != kMcsGuestId);
  return block_index;
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::ownerless_acquire_unconditional_ww(McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient mfences (note, xchg implies lock prefix. not a compiler's bug!).
  assert_mcs_aligned(mcs_lock);
  uint32_t* address = &(mcs_lock->data_);
  assert_mcs_aligned(address);
  spin_until([mcs_lock, address]{
    uint32_t old_int = McsLock::to_int(0, 0);
    return !assorted::raw_atomic_compare_exchange_weak<uint32_t>(
      address,
      &old_int,
      kMcsGuestId);
  });
  DVLOG(1) << "Okay, now I hold the lock. me=guest";
  ASSERT_ND(mcs_lock->is_locked());
}

template <typename ADAPTOR>
McsBlockIndex McsImpl<ADAPTOR>::initial_ww(McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with release barrier.
  // This method itself doesn't need barriers, but then we need to later take a seq_cst barrier
  // in an appropriate place. That's hard to debug, so just take release barriers here.
  // Also, everything should be inlined.
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(!adaptor_.me_waiting()->load());
  ASSERT_ND(!mcs_lock->is_locked());
  // so far we allow only 2^16 MCS blocks per transaction. we might increase later.
  ASSERT_ND(adaptor_.get_cur_block() < 0xFFFFU);

  McsBlockIndex block_index = adaptor_.issue_new_block();
  ASSERT_ND(block_index > 0 && block_index <= 0xFFFFU);
  McsBlock* my_block = adaptor_.get_ww_my_block(block_index);
  my_block->clear_successor_release();
  const thread::ThreadId id = adaptor_.get_my_id();
  mcs_lock->reset_release(id, block_index);
  return block_index;
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::ownerless_initial_ww(McsLock* mcs_lock) {
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(!mcs_lock->is_locked());
  mcs_lock->reset_guest_id_release();
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::release_ww(McsLock* mcs_lock, McsBlockIndex block_index) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient lock/mfences.
  assert_mcs_aligned(mcs_lock);
  ASSERT_ND(!adaptor_.me_waiting()->load());
  ASSERT_ND(mcs_lock->is_locked());
  ASSERT_ND(block_index > 0);
  ASSERT_ND(adaptor_.get_cur_block() >= block_index);
  const thread::ThreadId id = adaptor_.get_my_id();
  const uint32_t myself = McsLock::to_int(id, block_index);
  uint32_t* address = &(mcs_lock->data_);
  McsBlock* block = adaptor_.get_ww_my_block(block_index);
  if (!block->has_successor()) {
    // okay, successor "seems" nullptr (not contended), but we have to make it sure with atomic CAS
    uint32_t expected = myself;
    assert_mcs_aligned(address);
    bool swapped = assorted::raw_atomic_compare_exchange_strong<uint32_t>(address, &expected, 0);
    if (swapped) {
      // we have just unset the locked flag, but someone else might have just acquired it,
      // so we can't put assertion here.
      ASSERT_ND(id == 0 || mcs_lock->get_tail_waiter() != id);
      ASSERT_ND(expected == myself);
      ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);
      DVLOG(2) << "Okay, release a lock uncontended. me=" << id;
      return;
    }
    ASSERT_ND(expected != 0);
    ASSERT_ND(expected != kMcsGuestId);
    DVLOG(0) << "Interesting contention on MCS release. I thought it's null, but someone has just "
      " jumped in. me=" << id << ", mcs_lock=" << *mcs_lock;
    // wait for someone else to set the successor
    ASSERT_ND(mcs_lock->is_locked());
    if (UNLIKELY(!block->has_successor())) {
      spin_until([block]{ return !block->has_successor_atomic(); });
    }
  }
  thread::ThreadId successor_id = block->get_successor_thread_id();
  DVLOG(1) << "Okay, I have a successor. me=" << id << ", succ=" << successor_id;
  ASSERT_ND(successor_id != id);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);

  ASSERT_ND(adaptor_.get_other_cur_block(successor_id) >= block->get_successor_block());
  ASSERT_ND(adaptor_.other_waiting(successor_id)->load());
  ASSERT_ND(mcs_lock->is_locked());

  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);
  adaptor_.other_waiting(successor_id)->store(false, std::memory_order_release);
  ASSERT_ND(assorted::atomic_load_seq_cst<uint32_t>(address) != myself);
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::ownerless_release_ww(McsLock* mcs_lock) {
  // Basically _all_ writes in this function must come with some memory barrier. Be careful!
  // Also, the performance of this method really matters, especially that of common path.
  // Check objdump -d. Everything in common path should be inlined.
  // Also, check minimal sufficient mfences (note, xchg implies lock prefix. not a compiler's bug!).
  assert_mcs_aligned(mcs_lock);
  uint32_t* address = &(mcs_lock->data_);
  assert_mcs_aligned(address);
  ASSERT_ND(mcs_lock->is_locked());
  spin_until([address]{
    uint32_t old_int = kMcsGuestId;
    return !assorted::raw_atomic_compare_exchange_weak<uint32_t>(address, &old_int, 0);
  });
  DVLOG(1) << "Okay, guest released the lock.";
}

////////////////////////////////////////////////////////////////////////////////
///
///      The Simple MCS-RW lock
///
////////////////////////////////////////////////////////////////////////////////
template <typename ADAPTOR>
bool McsImpl<ADAPTOR>::acquire_try_rw_writer_simple(
  McsRwLock* lock,
  McsBlockIndex* out_block_index) {
  const thread::ThreadId id = adaptor_.get_my_id();
  McsBlockIndex block_index = 0;
  if (*out_block_index) {
    block_index = *out_block_index;
  } else {
    block_index = *out_block_index = adaptor_.issue_new_block();
  }
  auto* my_block = adaptor_.get_rw_my_block(block_index);
  my_block->init_writer();

  McsRwLock tmp;
  uint64_t expected = *reinterpret_cast<uint64_t*>(&tmp);
  McsRwLock tmp2;
  tmp2.tail_ = McsRwLock::to_tail_int(id, block_index);
  uint64_t desired = *reinterpret_cast<uint64_t*>(&tmp2);
  my_block->unblock();
  return assorted::raw_atomic_compare_exchange_weak<uint64_t>(
    reinterpret_cast<uint64_t*>(lock), &expected, desired);
}

template <typename ADAPTOR>
bool McsImpl<ADAPTOR>::acquire_try_rw_reader_simple(
  McsRwLock* lock,
  McsBlockIndex* out_block_index) {
  const thread::ThreadId id = adaptor_.get_my_id();
  while (true) {
    // take a look at the whole lock word, and cas if it's a reader or null
    uint64_t lock_word = assorted::atomic_load_acquire<uint64_t>(reinterpret_cast<uint64_t*>(lock));
    McsRwLock ll;
    memcpy(&ll, &lock_word, sizeof(ll));
    if (ll.next_writer_ != McsRwLock::kNextWriterNone) {
      return false;
    }
    McsRwBlock* block = NULL;
    if (ll.tail_) {
      block = dereference_rw_tail_block(ll.tail_);
    }
    if (ll.tail_ == 0 || (block->is_granted() && block->is_reader())) {
      McsBlockIndex block_index = 0;
      if (*out_block_index) {
        block_index = *out_block_index;
      } else {
        block_index = *out_block_index = adaptor_.issue_new_block();
      }
      ll.increment_readers_count();
      ll.tail_ = McsRwLock::to_tail_int(id, block_index);
      uint64_t desired = *reinterpret_cast<uint64_t*>(&ll);
      auto* my_block = adaptor_.get_rw_my_block(block_index);
      my_block->init_reader();

      if (assorted::raw_atomic_compare_exchange_weak<uint64_t>(
        reinterpret_cast<uint64_t*>(lock), &lock_word, desired)) {
        if (block) {
          block->set_successor_next_only(id, block_index);
        }
        my_block->unblock();
        return true;
      }
    }
  }
}

template <typename ADAPTOR>
bool McsImpl<ADAPTOR>::acquire_try_rw_writer_upgrade_simple(
  McsRwLock* lock,
  McsBlockIndex* out_block_index) {
  // This try_upgrade is a bit special.
  // We create a new queue node, discarding the one we have already pushed to the tail.
  // It is safe to do such a thing only when there are no other waiters/owners at all,
  // so we CAS against the condition.
  const thread::ThreadId id = adaptor_.get_my_id();
  McsBlockIndex cur_block_index = *out_block_index;
  ASSERT_ND(cur_block_index);
  McsBlockIndex new_block_index = adaptor_.issue_new_block();
  auto* my_block = adaptor_.get_rw_my_block(new_block_index);
  my_block->init_writer();

  // No writer, no other reader (1=myself), so surely no successor to my old queue node
  McsRwLock tmp;
  tmp.tail_ = McsRwLock::to_tail_int(id, cur_block_index);
  tmp.readers_count_ = 1U;
  uint64_t expected = *reinterpret_cast<uint64_t*>(&tmp);
  McsRwLock tmp2;
  tmp2.tail_ = McsRwLock::to_tail_int(id, new_block_index);
  uint64_t desired = *reinterpret_cast<uint64_t*>(&tmp2);
  my_block->unblock();
  if (assorted::raw_atomic_compare_exchange_weak<uint64_t>(
    reinterpret_cast<uint64_t*>(lock), &expected, desired)) {
    *out_block_index = new_block_index;
    return true;
  } else {
    return false;
  }
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::acquire_unconditional_rw_reader_simple(
  McsRwLock* mcs_rw_lock,
  McsBlockIndex* out_block_index) {
  ASSERT_ND(adaptor_.get_cur_block() < 0xFFFFU);
  const thread::ThreadId id = adaptor_.get_my_id();
  McsBlockIndex block_index = 0;
  if (*out_block_index) {
    block_index = *out_block_index;
  } else {
    block_index = *out_block_index = adaptor_.issue_new_block();
  }
  ASSERT_ND(block_index > 0);
  // TODO(tzwang): make this a static_size_check...
  ASSERT_ND(sizeof(McsRwBlock) == sizeof(McsBlock));
  McsRwBlock* my_block = adaptor_.get_rw_my_block(block_index);

  // So I'm a reader
  my_block->init_reader();
  ASSERT_ND(my_block->is_blocked() && my_block->is_reader());
  ASSERT_ND(!my_block->has_successor());
  ASSERT_ND(my_block->successor_block_index_ == 0);

  // Now ready to XCHG
  uint32_t tail_desired = McsRwLock::to_tail_int(id, block_index);
  uint32_t* tail_address = &(mcs_rw_lock->tail_);
  uint32_t pred_tail_int = assorted::raw_atomic_exchange<uint32_t>(tail_address, tail_desired);

  if (pred_tail_int == 0) {
    mcs_rw_lock->increment_readers_count();
    my_block->unblock();  // reader successors will know they don't need to wait
  } else {
    // See if the predecessor is a reader; if so, if it already acquired the lock.
    McsRwBlock* pred_block = dereference_rw_tail_block(pred_tail_int);
    uint16_t* pred_state_address = &pred_block->self_.data_;
    uint16_t pred_state_expected = pred_block->make_blocked_with_no_successor_state();
    uint16_t pred_state_desired = pred_block->make_blocked_with_reader_successor_state();
    if (!pred_block->is_reader() || assorted::raw_atomic_compare_exchange_strong<uint16_t>(
      pred_state_address,
      &pred_state_expected,
      pred_state_desired)) {
      // Predecessor is a writer or a waiting reader. The successor class field and the
      // blocked state in pred_block are separated, so we can blindly set_successor().
      pred_block->set_successor_next_only(id, block_index);
      spin_until([my_block]{ return my_block->is_granted(); });
    } else {
      // Join the active, reader predecessor
      ASSERT_ND(!pred_block->is_blocked());
      mcs_rw_lock->increment_readers_count();
      pred_block->set_successor_next_only(id, block_index);
      my_block->unblock();
    }
  }
  finalize_acquire_reader_simple(mcs_rw_lock, my_block);
  ASSERT_ND(my_block->is_finalized());
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::finalize_acquire_reader_simple(
  McsRwLock* mcs_rw_lock,
  McsRwBlock* my_block) {
  ASSERT_ND(!my_block->is_finalized());
  if (my_block->has_reader_successor()) {
    spin_until([my_block]{ return !my_block->successor_is_ready(); });
    // Unblock the reader successor
    McsRwBlock* successor_block = adaptor_.get_rw_other_block(
      my_block->successor_thread_id_,
      my_block->successor_block_index_);
    mcs_rw_lock->increment_readers_count();
    successor_block->unblock();
  }
  my_block->set_finalized();
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::release_rw_reader_simple(
  McsRwLock* mcs_rw_lock,
  McsBlockIndex block_index) {
  const thread::ThreadId id = adaptor_.get_my_id();
  ASSERT_ND(block_index > 0);
  ASSERT_ND(adaptor_.get_cur_block() >= block_index);
  McsRwBlock* my_block = adaptor_.get_rw_my_block(block_index);
  ASSERT_ND(my_block->is_finalized());
  // Make sure there is really no successor or wait for it
  uint32_t* tail_address = &mcs_rw_lock->tail_;
  uint32_t expected = McsRwLock::to_tail_int(id, block_index);
  if (my_block->successor_is_ready() ||
    !assorted::raw_atomic_compare_exchange_strong<uint32_t>(tail_address, &expected, 0)) {
    // Have to wait for the successor to install itself after me
    // Don't check for curr_block->has_successor()! It only tells whether the state bit
    // is set, not whether successor_thread_id_ and successor_block_index_ are set.
    // But remember to skip trying readers who failed.
    spin_until([my_block]{ return !(my_block->successor_is_ready()); });
    if (my_block->has_writer_successor()) {
      assorted::raw_atomic_exchange<thread::ThreadId>(
        &mcs_rw_lock->next_writer_,
        my_block->successor_thread_id_);
    }
  }

  if (mcs_rw_lock->decrement_readers_count() == 1) {
    // I'm the last active reader
    thread::ThreadId next_writer
      = assorted::atomic_load_acquire<thread::ThreadId>(&mcs_rw_lock->next_writer_);
    if (next_writer != McsRwLock::kNextWriterNone &&
        mcs_rw_lock->nreaders() == 0 &&
        assorted::raw_atomic_compare_exchange_strong<thread::ThreadId>(
          &mcs_rw_lock->next_writer_,
          &next_writer,
          McsRwLock::kNextWriterNone)) {
      // I have a waiting writer, wake it up
      // Assuming a thread can wait for one and only one MCS lock at any instant
      // before starting to acquire the next.
      McsBlockIndex next_cur_block = adaptor_.get_other_cur_block(next_writer);
      McsRwBlock *writer_block = adaptor_.get_rw_other_block(next_writer, next_cur_block);
      ASSERT_ND(writer_block->is_blocked());
      ASSERT_ND(!writer_block->is_reader());
      writer_block->unblock();
    }
  }
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::acquire_unconditional_rw_writer_simple(
  McsRwLock* mcs_rw_lock,
  McsBlockIndex* out_block_index) {
  const thread::ThreadId id = adaptor_.get_my_id();
  McsBlockIndex block_index = 0;
  if (*out_block_index) {
    block_index = *out_block_index;
  } else {
    block_index = *out_block_index = adaptor_.issue_new_block();
  }
  ASSERT_ND(adaptor_.get_cur_block() < 0xFFFFU);
  ASSERT_ND(block_index > 0);
  // TODO(tzwang): make this a static_size_check...
  ASSERT_ND(sizeof(McsRwBlock) == sizeof(McsBlock));
  McsRwBlock* my_block = adaptor_.get_rw_my_block(block_index);

  my_block->init_writer();
  ASSERT_ND(my_block->is_blocked() && !my_block->is_reader());
  ASSERT_ND(!my_block->has_successor());
  ASSERT_ND(my_block->successor_block_index_ == 0);

  // Now ready to XCHG
  uint32_t tail_desired = McsRwLock::to_tail_int(id, block_index);
  uint32_t* tail_address = &(mcs_rw_lock->tail_);
  uint32_t pred_tail_int = assorted::raw_atomic_exchange<uint32_t>(tail_address, tail_desired);
  ASSERT_ND(pred_tail_int != tail_desired);
  thread::ThreadId old_next_writer = 0xFFFFU;
  if (pred_tail_int == 0) {
    assorted::raw_atomic_exchange<thread::ThreadId>(&mcs_rw_lock->next_writer_, id);
    if (mcs_rw_lock->nreaders() == 0) {
      old_next_writer = assorted::raw_atomic_exchange<thread::ThreadId>(
        &mcs_rw_lock->next_writer_,
        McsRwLock::kNextWriterNone);
      if (old_next_writer == id) {
        my_block->unblock();
        return;
      }
    }
  } else {
    McsRwBlock* pred_block = dereference_rw_tail_block(pred_tail_int);
    pred_block->set_successor_class_writer();
    pred_block->set_successor_next_only(id, block_index);
  }
  spin_until([my_block]{ return my_block->is_granted(); });
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::release_rw_writer_simple(
  McsRwLock* mcs_rw_lock,
  McsBlockIndex block_index) {
  const thread::ThreadId id = adaptor_.get_my_id();
  ASSERT_ND(block_index > 0);
  ASSERT_ND(adaptor_.get_cur_block() >= block_index);
  McsRwBlock* my_block = adaptor_.get_rw_my_block(block_index);
  uint32_t expected = McsRwLock::to_tail_int(id, block_index);
  uint32_t* tail_address = &mcs_rw_lock->tail_;
  if (my_block->successor_is_ready() ||
    !assorted::raw_atomic_compare_exchange_strong<uint32_t>(tail_address, &expected, 0)) {
    if (UNLIKELY(!my_block->successor_is_ready())) {
      spin_until([my_block]{ return !(my_block->successor_is_ready()); });
    }
    ASSERT_ND(my_block->successor_is_ready());
    auto* successor_block = adaptor_.get_rw_other_block(
      my_block->successor_thread_id_,
      my_block->successor_block_index_);
    ASSERT_ND(successor_block->is_blocked());
    if (successor_block->is_reader()) {
      mcs_rw_lock->increment_readers_count();
    }
    successor_block->unblock();
  }
}

////////////////////////////////////////////////////////////////////////////////
///
///      The Extended MCS-RW lock. This is so far a placeholder.
///
////////////////////////////////////////////////////////////////////////////////
template <typename ADAPTOR>
bool McsImpl<ADAPTOR>::acquire_try_rw_writer_extended(
  McsRwLock* /*lock*/,
  McsBlockIndex* /*out*/) {
  return true;
}

template <typename ADAPTOR>
bool McsImpl<ADAPTOR>::acquire_try_rw_reader_extended(
  McsRwLock* /*lock*/,
  McsBlockIndex* /*out*/) {
  return true;
}

template <typename ADAPTOR>
bool McsImpl<ADAPTOR>::acquire_try_rw_writer_upgrade_extended(
  McsRwLock* /*lock*/,
  McsBlockIndex* /*out*/) {
  return true;
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::acquire_unconditional_rw_reader_extended(
  McsRwLock* /*lock*/,
  McsBlockIndex* /*out*/) {
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::release_rw_reader_extended(
  McsRwLock* /*lock*/,
  McsBlockIndex /*block_index*/) {
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::acquire_unconditional_rw_writer_extended(
  McsRwLock* /*lock*/,
  McsBlockIndex* /*out*/) {
}

template <typename ADAPTOR>
void McsImpl<ADAPTOR>::release_rw_writer_extended(
  McsRwLock* /*lock*/,
  McsBlockIndex /*block_index*/) {
}


/// Finally, explicit instantiation of the template class.
/// We instantiate the real adaptor for ThreadPimpl and the mock one for testing.
template class McsImpl<McsMockAdaptor>;

}  // namespace xct
}  // namespace foedus
