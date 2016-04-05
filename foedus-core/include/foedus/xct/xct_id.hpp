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
#ifndef FOEDUS_XCT_XCT_ID_HPP_
#define FOEDUS_XCT_XCT_ID_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"

/**
 * @file foedus/xct/xct_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup XCT
 */
namespace foedus {
namespace xct {

/**
 * @brief Specifies the level of isolation during transaction processing.
 * @ingroup XCT
 * @details
 * May add:
 * \li COMMITTED_READ: see-epoch and read data -> fence -> check-epoch, then forget the read set
 * \li REPEATABLE_READ: assuming no-repeated-access (which we do assume), same as COMMITTED_READ
 *
 * but probably they are superseded either by kDirtyRead or kSnapshot.
 */
enum IsolationLevel {
  /**
   * @brief No guarantee at all for reads, for the sake of best performance and scalability.
   * @details
   * This avoids checking and even storing read set, thus provides the best performance.
   * However, concurrent transactions might be modifying the data the transaction is now reading.
   * So, this has a chance of reading half-changed data.
   * This mode prefers volatile pages if both a snapshot page and a volatile page is available.
   * In other words, more recent but more inconsistent reads compared to kSnapshot.
   */
  kDirtyRead,

  /**
   * @brief Snapshot isolation (SI), meaning the transaction reads a consistent and complete image
   * of the database as of the previous snapshot.
   * @details
   * Writes are same as kSerializable, but all reads
   * simply follow snapshot-pointer from the root, so there is no race, no abort, no verification.
   * Hence, higher scalability than kSerializable.
   * However, this level can result in \e write \e skews.
   * Choose this level if you want highly consistent reads and very high performance.
   * TASK(Hideaki): Allow specifying which snapshot we should be based on. Low priority.
   */
  kSnapshot,

  /**
   * @brief Protects against all anomalies in all situations.
   * @details
   * This is the most expensive level, but everything good has a price.
   * Choose this level if you want full correctness.
   */
  kSerializable,
};

/**
 * @brief Represents a mode of lock.
 * @ingroup XCT
 * @details
 * The order is important. The larger lock mode includes the smaller.
 */
enum LockMode {
  /**
   * taken_mode_: Not taken the lock yet.
   * preferred_mode_: Implies that we shouldn't take any lock on this entry in next run.
   */
  kNoLock = 0,
  /**
   * taken_mode_: we took a read-lock, \b not write-lock yet.
   * preferred_mode_: Implies that we should take a read-lock on this entry in next run.
   */
  kReadLock,
  /**
   * taken_mode_: we took a write-lock.
   * preferred_mode_: Implies that we should take a write-lock on this entry in next run.
   */
  kWriteLock,
};

/**
 * @brief Universally ordered identifier of each lock
 * @ingroup XCT
 * @details
 * This must follow a universally consistent order even across processes.
 *
 * Currently we assume all locks come from volatile pages. The bits are used as:
 * |--63--48--|--47---0--|
 * |--NodeId--|--Offset--|
 *
 * NodeId: ID of the NUMA node where this lock is allocated.
 * Offset: the lock VA's offset based off of its owner node's virtual pool's start VA.
 *
 * We attach the same shmem in fresh new processes in the same order
 * and in the same machine.. most likely we get the same VA-mapping.
 * // ASLR? Turn it off. I don't care security.
 *
 * So far a unitptr_t, revisit later if we need a struct over a uint64_t. Now we
 * just compare the whole uintptr, which might cause some socket's data are always
 * later; ideally, we can compare only the offset part. Revisit later.
 */
typedef uintptr_t UniversalLockId;

/** This never points to a valid lock, and also evaluates less than any vaild alocks */
const UniversalLockId kNullUniversalLockId = 0;

/**
 * @brief Index in a lock-list, either RLL or CLL.
 * @ingroup XCT
 * @details
 * The value zero is guaranteed to be invalid.
 * So, lock lists using this type must reserve index-0 to be either a dummy entry or some
 * sentinel entry. Thanks to this contract, it's easy to initialize structs holding this type.
 * @see kLockListPositionNull
 */
typedef uint32_t LockListPosition;
const LockListPosition kLockListPositionInvalid = 0;


/** Index in thread-local MCS block. 0 means not locked. */
typedef uint32_t McsBlockIndex;
/**
 * A special value meaning the lock is held by a non-regular guest
 * that doesn't have a context. See the MCSg paper for more details.
 */
const uint64_t kMcsGuestId = -1;

/** Return value of acquire_async_rw. */
struct AcquireAsyncRet {
  /** whether we immediately acquired the lock or not */
  bool acquired_;
  /**
   * the queue node we pushed.
   * It is always set whether acquired_ or not, whether simple or extended.
   * However, in simple case when !acquired_, the block is not used and nothing
   * sticks to the queue. We just skip the index next time.
   */
  McsBlockIndex block_index_;
};

/////////////////////////////////////////////////////////////////////////////
///
/// Exclusive-only (WW) MCS lock classes.
/// These are mainly used for page locks.
///
/////////////////////////////////////////////////////////////////////////////
/**
 * Represents an exclusive-only MCS node, a pair of node-owner (thread) and its block index.
 */
struct McsBlockData {
  /**
   * The high 32-bits is thread_id, the low 32-bit is block-index.
   * We so far need only 16-bits each, but reserved more bits for future use.
   * We previously used union for this, but it caused many "accidentally non-access-once" bugs.
   * We thus avoid using union. Not saying that union is wrong, but it's prone to such coding.
   */
  uint64_t word_;

  static uint64_t combine(uint32_t thread_id, McsBlockIndex block) ALWAYS_INLINE {
    uint64_t word = thread_id;
    word <<= 32;
    word |= block;
    return word;
  }
  static uint32_t decompose_thread_id(uint64_t word) ALWAYS_INLINE {
    return static_cast<uint32_t>((word >> 32) & 0xFFFFFFFFUL);
  }
  static McsBlockIndex decompose_block(uint64_t word) ALWAYS_INLINE {
    return static_cast<McsBlockIndex>(word & 0xFFFFFFFFUL);
  }

  McsBlockData() : word_(0) {
  }
  explicit McsBlockData(uint64_t word) : word_(word) {
  }
  McsBlockData(uint32_t thread_id, McsBlockIndex block) {
    set_relaxed(thread_id, block);
  }
  bool operator==(const McsBlockData& other) const {
    return word_ == other.word_;
  }
  bool operator!=(const McsBlockData& other) const {
    return word_ != other.word_;
  }

  uint64_t get_word_acquire() const ALWAYS_INLINE {
    return assorted::atomic_load_acquire<uint64_t>(&word_);
  }
  uint64_t get_word_consume() const ALWAYS_INLINE {
    return assorted::atomic_load_consume<uint64_t>(&word_);
  }
  uint64_t get_word_atomic() const ALWAYS_INLINE {
    return assorted::atomic_load_seq_cst<uint64_t>(&word_);
  }
  /**
   * The access_once semantics, which is widely used in linux.
   * This is weaker than get_word_atomic(), but enough for many places.
   */
  uint64_t get_word_once() const ALWAYS_INLINE { return *(&word_); }
  McsBlockData copy_once() const ALWAYS_INLINE { return McsBlockData(get_word_once()); }
  McsBlockData copy_consume() const ALWAYS_INLINE { return McsBlockData(get_word_consume()); }
  McsBlockData copy_acquire() const ALWAYS_INLINE { return McsBlockData(get_word_acquire()); }
  McsBlockData copy_atomic() const ALWAYS_INLINE { return McsBlockData(get_word_atomic()); }

  bool is_valid() const ALWAYS_INLINE { return word_ != 0; }
  bool is_valid_relaxed() const ALWAYS_INLINE { return word_ != 0; }
  bool is_valid_consume() const ALWAYS_INLINE { return get_word_consume() == 0; }
  bool is_valid_acquire() const ALWAYS_INLINE { return get_word_acquire() == 0; }
  bool is_valid_atomic() const ALWAYS_INLINE {
    return get_word_atomic() != 0;
  }
  bool is_guest_relaxed() const ALWAYS_INLINE { return word_ == kMcsGuestId; }
  bool is_guest_acquire() const ALWAYS_INLINE {
    return get_word_acquire() == kMcsGuestId;
  }
  bool is_guest_consume() const ALWAYS_INLINE {
    return get_word_consume() == kMcsGuestId;
  }
  bool is_guest_atomic() const ALWAYS_INLINE {
    return get_word_atomic() == kMcsGuestId;
  }
  /**
   * Carefully use this! In some places you must call get_word_once() then call this on the copy.
   * We thus put "_relaxed" as suffix.
   */
  inline uint32_t get_thread_id_relaxed() const ALWAYS_INLINE {
    return McsBlockData::decompose_thread_id(word_);
  }
  /**
   * Carefully use this! In some places you must call get_word_once() then call this on the copy.
   * We thus put "_relaxed" as suffix.
   */
  inline McsBlockIndex  get_block_relaxed() const ALWAYS_INLINE {
    return McsBlockData::decompose_block(word_);
  }
  void clear() ALWAYS_INLINE { word_ = 0; }
  void clear_atomic() ALWAYS_INLINE {
    assorted::atomic_store_seq_cst<uint64_t>(&word_, 0);
  }
  void clear_release() ALWAYS_INLINE {
    assorted::atomic_store_release<uint64_t>(&word_, 0);
  }
  void set_relaxed(uint32_t thread_id, McsBlockIndex block) ALWAYS_INLINE {
    word_ = McsBlockData::combine(thread_id, block);
  }
  void set_atomic(uint32_t thread_id, McsBlockIndex block) ALWAYS_INLINE {
    set_combined_atomic(McsBlockData::combine(thread_id, block));
  }
  void set_release(uint32_t thread_id, McsBlockIndex block) ALWAYS_INLINE {
    set_combined_release(McsBlockData::combine(thread_id, block));
  }
  void set_combined_atomic(uint64_t word) ALWAYS_INLINE {
    assorted::atomic_store_seq_cst<uint64_t>(&word_, word);
  }
  void set_combined_release(uint64_t word) ALWAYS_INLINE {
    assorted::atomic_store_release<uint64_t>(&word_, word);
  }
};

/** Pre-allocated MCS block for WW-locks. we so far pre-allocate at most 2^16 nodes per thread. */
struct McsBlock {
  /**
   * The successor of MCS lock queue after this thread (in other words, the thread that is
   * waiting for this thread). Successor is represented by thread ID and block,
   * the index in mcs_blocks_.
   */
  McsBlockData successor_;

  /// setter/getter for successor_.
  inline bool has_successor_relaxed() const ALWAYS_INLINE { return successor_.is_valid_relaxed(); }
  inline bool has_successor_consume() const ALWAYS_INLINE { return successor_.is_valid_consume(); }
  inline bool has_successor_acquire() const ALWAYS_INLINE { return successor_.is_valid_acquire(); }
  inline bool has_successor_atomic() const ALWAYS_INLINE { return successor_.is_valid_atomic(); }
  /**
   * Carefully use this! In some places you must call copy_once() then call this on the copy.
   * We thus put "_relaxed" as suffix.
   */
  inline uint32_t         get_successor_thread_id_relaxed() const ALWAYS_INLINE {
    return successor_.get_thread_id_relaxed();
  }
  /**
   * Carefully use this! In some places you must call copy_once() then call this on the copy.
   * We thus put "_relaxed" as suffix.
   */
  inline McsBlockIndex    get_successor_block_relaxed() const ALWAYS_INLINE {
    return successor_.get_block_relaxed();
  }
  inline void             clear_successor_atomic() ALWAYS_INLINE { successor_.clear_atomic(); }
  inline void             clear_successor_release() ALWAYS_INLINE { successor_.clear_release(); }
  inline void set_successor_atomic(thread::ThreadId thread_id, McsBlockIndex block) ALWAYS_INLINE {
    successor_.set_atomic(thread_id, block);
  }
  inline void set_successor_release(thread::ThreadId thread_id, McsBlockIndex block) ALWAYS_INLINE {
    successor_.set_release(thread_id, block);
  }
};

/**
 * @brief An exclusive-only (WW) MCS lock data structure.
 * @ingroup XCT
 * @details
 * This is the minimal unit of locking in our system.
 * Unlike SILO, we employ MCS locking that scales much better on big machines.
 * This object stores \e tail-waiter, which indicates the thread that is in the tail of the queue
 * lock, which \e might be the owner of the lock.
 * The MCS-lock nodes are pre-allocated for each thread and placed in shared memory.
 *
 * This is the original MCSg lock implementation without cancel/RW functionality.
 * It has the guest functionality used by background threads to take page locks.
 */
struct McsLock {
  McsLock() { tail_.clear(); }
  McsLock(thread::ThreadId tail_waiter, McsBlockIndex tail_waiter_block) {
    tail_.set_relaxed(tail_waiter, tail_waiter_block);
  }

  McsLock(const McsLock& other) CXX11_FUNC_DELETE;
  McsLock& operator=(const McsLock& other) CXX11_FUNC_DELETE;

  /** Used only for sanity check */
  uint8_t   last_1byte_addr() const ALWAYS_INLINE {
    // address is surely a multiply of 4. omit that part.
    ASSERT_ND(reinterpret_cast<uintptr_t>(reinterpret_cast<const void*>(this)) % 4 == 0);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<const void*>(this)) / 4;
  }
  /** This is a "relaxed" check. Use with caution. */
  bool      is_locked() const { return tail_.is_valid(); }

  /** Equivalent to context->mcs_acquire_lock(this). Actually that's more preferred. */
  McsBlockIndex acquire_lock(thread::Thread* context);
  /** This doesn't use any atomic operation to take a lock. only allowed when there is no race */
  McsBlockIndex initial_lock(thread::Thread* context);
  /** Equivalent to context->mcs_release_lock(this). Actually that's more preferred. */
  void          release_lock(thread::Thread* context, McsBlockIndex block);

  /// The followings are implemented in thread_pimpl.cpp along with the above methods,
  /// but these don't use any of Thread's context information.
  void          ownerless_initial_lock();
  void          ownerless_acquire_lock();
  void          ownerless_release_lock();


  /** This is a "relaxed" check. Use with caution. */
  thread::ThreadId get_tail_waiter() const ALWAYS_INLINE { return tail_.get_thread_id_relaxed(); }
  /** This is a "relaxed" check. Use with caution. */
  McsBlockIndex get_tail_waiter_block() const ALWAYS_INLINE { return tail_.get_block_relaxed(); }

  McsBlockData get_tail_relaxed() const ALWAYS_INLINE { return tail_; }
  McsBlockData get_tail_once() const ALWAYS_INLINE { return tail_.copy_once(); }
  McsBlockData get_tail_consume() const ALWAYS_INLINE { return tail_.copy_consume(); }
  McsBlockData get_tail_acquire() const ALWAYS_INLINE { return tail_.copy_acquire(); }
  McsBlockData get_tail_atomic() const ALWAYS_INLINE { return tail_.copy_atomic(); }

  /** used only while page initialization */
  void  reset() ALWAYS_INLINE { tail_.clear(); }

  void  reset_guest_id_release() {
    tail_.set_combined_release(kMcsGuestId);
  }

  /** used only for initial_lock() */
  void  reset(thread::ThreadId tail_waiter, McsBlockIndex tail_waiter_block) ALWAYS_INLINE {
    tail_.set_relaxed(tail_waiter, tail_waiter_block);
  }

  void  reset_atomic() ALWAYS_INLINE { reset_atomic(0, 0); }
  void  reset_atomic(thread::ThreadId tail_waiter, McsBlockIndex tail_waiter_block) ALWAYS_INLINE {
    tail_.set_atomic(tail_waiter, tail_waiter_block);
  }
  void  reset_release() ALWAYS_INLINE { reset_release(0, 0); }
  void  reset_release(thread::ThreadId tail_waiter, McsBlockIndex tail_waiter_block) ALWAYS_INLINE {
    tail_.set_release(tail_waiter, tail_waiter_block);
  }

  friend std::ostream& operator<<(std::ostream& o, const McsLock& v);

  McsBlockData tail_;
};

/////////////////////////////////////////////////////////////////////////////
///
/// Reader-writer (RW) MCS lock classes.
/// These are mainly used for record locks.
///
/////////////////////////////////////////////////////////////////////////////
/** Pre-allocated MCS block for simpler version of RW-locks. */
struct McsRwSimpleBlock {
  static const uint8_t kStateClassMask       = 3U;        // [LSB + 1, LSB + 2]
  static const uint8_t kStateClassReaderFlag = 1U;        // LSB binary = 01
  static const uint8_t kStateClassWriterFlag = 2U;        // LSB binary = 10

  static const uint8_t kStateBlockedFlag     = 1U << 7U;  // MSB binary = 1
  static const uint8_t kStateBlockedMask     = 1U << 7U;

  static const uint8_t kStateFinalizedMask   = 4U;

  static const uint8_t kSuccessorClassReader = 1U;
  static const uint8_t kSuccessorClassWriter = 2U;
  static const uint8_t kSuccessorClassNone   = 3U;        // LSB binary 11

  static const int32_t kTimeoutNever         = 0xFFFFFFFF;

  union Self {
    uint16_t data_;                       // +2 => 2
    struct Components {
      uint8_t successor_class_;
      // state_ covers:
      // Bit 0-1: my **own** class (am I a reader or writer?)
      // Bit 2: whether we have checked the successor ("finalized", for readers only)
      // Bit 7: blocked (am I waiting for the lock or acquired?)
      uint8_t state_;
    } components_;
  } self_;
  // TODO(tzwang): make these two fields 8 bytes by themselves. Now we need
  // to worry about sub-word writes (ie have to use atomic ops even when
  // changing only these two fields because they are in the same byte as data_).
  thread::ThreadId successor_thread_id_;  // +2 => 4
  McsBlockIndex successor_block_index_;   // +4 => 8

  inline void init_reader() {
    self_.components_.state_ = kStateClassReaderFlag | kStateBlockedFlag;
    init_common();
  }
  inline void init_writer() {
    self_.components_.state_ = kStateClassWriterFlag | kStateBlockedFlag;
    init_common();
  }
  inline void init_common() ALWAYS_INLINE {
    self_.components_.successor_class_ = kSuccessorClassNone;
    successor_thread_id_ = 0;
    successor_block_index_ = 0;
    assorted::memory_fence_release();
  }

  inline bool is_reader() ALWAYS_INLINE {
    return (self_.components_.state_ & kStateClassMask) == kStateClassReaderFlag;
  }
  inline uint8_t read_state() {
    return assorted::atomic_load_acquire<uint8_t>(&self_.components_.state_);
  }
  inline void unblock() ALWAYS_INLINE {
    ASSERT_ND(read_state() & kStateBlockedFlag);
    assorted::raw_atomic_fetch_and_bitwise_and<uint8_t>(
      &self_.components_.state_,
      static_cast<uint8_t>(~kStateBlockedMask));
  }
  inline bool is_blocked() ALWAYS_INLINE {
    return read_state() & kStateBlockedMask;
  }
  inline bool is_granted() {
    return !is_blocked();
  }
  inline void set_finalized() {
    ASSERT_ND(is_reader());
    ASSERT_ND(!is_finalized());
    assorted::raw_atomic_fetch_and_bitwise_or<uint8_t>(
      &self_.components_.state_, kStateFinalizedMask);
    ASSERT_ND(is_finalized());
  }
  inline bool is_finalized() {
    ASSERT_ND(is_reader());
    return read_state() & kStateFinalizedMask;
  }
  bool timeout_granted(int32_t timeout);
  inline void set_successor_class_writer() {
    // In case the caller is a reader appending after a writer or waiting reader,
    // the requester should have already set the successor class to "reader" through by CASing
    // self_.data_ from [no-successor, blocked] to [reader successor, blocked].
    ASSERT_ND(self_.components_.successor_class_ == kSuccessorClassNone);
    assorted::raw_atomic_fetch_and_bitwise_and<uint8_t>(
      &self_.components_.successor_class_, kSuccessorClassWriter);
  }
  inline void set_successor_next_only(thread::ThreadId thread_id, McsBlockIndex block_index) {
    McsRwSimpleBlock tmp;
    tmp.self_.data_ = 0;
    tmp.successor_thread_id_ = thread_id;
    tmp.successor_block_index_ = block_index;
    ASSERT_ND(successor_thread_id_ == 0);
    ASSERT_ND(successor_block_index_ == 0);
    uint64_t *address = reinterpret_cast<uint64_t*>(this);
    uint64_t mask = *reinterpret_cast<uint64_t*>(&tmp);
    assorted::raw_atomic_fetch_and_bitwise_or<uint64_t>(address, mask);
  }
  inline bool has_successor() {
    return assorted::atomic_load_acquire<uint8_t>(
      &self_.components_.successor_class_) != kSuccessorClassNone;
  }
  inline bool successor_is_ready() {
    // Check block index only - thread ID could be 0
    return assorted::atomic_load_acquire<McsBlockIndex>(&successor_block_index_) != 0;
  }
  inline bool has_reader_successor() {
    uint8_t s = assorted::atomic_load_acquire<uint8_t>(&self_.components_.successor_class_);
    return s == kSuccessorClassReader;
  }
  inline bool has_writer_successor() {
    uint8_t s = assorted::atomic_load_acquire<uint8_t>(&self_.components_.successor_class_);
    return s == kSuccessorClassWriter;
  }

  uint16_t make_blocked_with_reader_successor_state() {
    // Only using the class bit, which doesn't change, so no need to use atomic ops.
    uint8_t state = self_.components_.state_ | kStateBlockedFlag;
    return (uint16_t)state << 8 | kSuccessorClassReader;
  }
  uint16_t make_blocked_with_no_successor_state() {
    uint8_t state = self_.components_.state_ | kStateBlockedFlag;
    return (uint16_t)state << 8 | kSuccessorClassNone;
  }
};

/** Pre-allocated MCS block for extended version of RW-locks. */
struct McsRwExtendedBlock {
  /** Pred flags:
   *  |---31---|-----|---0---|
   *  |my class|empty|waiting|
   *
   *  Next flags:
   *  |-----31-30-----|-----|-2--|--1-0--|
   *  |successor class|empty|busy|waiting|
   */
  static const uint32_t kPredFlagWaiting         = 0U;
  static const uint32_t kPredFlagGranted         = 1U;
  static const uint32_t kPredFlagReader          = 0U;
  static const uint32_t kPredFlagWriter          = 1U << 31;
  static const uint32_t kPredFlagClassMask       = 1U << 31;

  static const uint32_t kSuccFlagWaiting         = 0U;
  static const uint32_t kSuccFlagLeaving         = 1U;
  static const uint32_t kSuccFlagDirectGranted   = 2U;
  static const uint32_t kSuccFlagLeavingGranted  = 3U;
  static const uint32_t kSuccFlagMask            = 3U;

  static const uint32_t kSuccFlagBusy            = 4U;

  static const uint32_t kSuccFlagSuccessorClassMask = 3U << 30;
  static const uint32_t kSuccFlagSuccessorReader    = 3U << 30;
  static const uint32_t kSuccFlagSuccessorNone      = 0U;
  static const uint32_t kSuccFlagSuccessorWriter    = 1U << 30;

  static const uint32_t kSuccIdSuccessorLeaving  = 0xFFFFFFFFU;
  static const uint32_t kSuccIdNoSuccessor       = 0xFFFFFFFEU;

  static const uint32_t kPredIdAcquired          = 0xFFFFFFFFU;

#ifndef NDEBUG
  static const uint64_t kSuccReleased            = 0xFFFFFFFFFFFFFFFFU;
#endif

  /* Special timeouts for instant return and unconditional acquire */
  static const int32_t kTimeoutNever = 0xFFFFFFFFU;
  static const int32_t kTimeoutZero  = 0U;

  union Field {
    uint64_t data_;
    struct Components {
      uint32_t flags_;
      uint32_t id_;
    } components_;
  };

  Field pred_;
  Field next_;

#ifndef NDEBUG
  inline void mark_released() {
    ASSERT_ND(pred_flag_is_granted());
    ASSERT_ND(next_flag_is_granted());
    set_next(kSuccReleased);
  }

  inline bool is_released() {
    return get_next() == kSuccReleased;
  }
#endif

  inline uint32_t read_pred_flags() {
    return assorted::atomic_load_acquire<uint32_t>(&pred_.components_.flags_);
  }
  inline uint32_t read_next_flags() {
    return assorted::atomic_load_acquire<uint32_t>(&next_.components_.flags_);
  }
  inline bool is_writer() { return read_pred_flags() & kPredFlagWriter; }
  inline bool is_reader() { return !is_writer(); }
  inline bool pred_flag_is_waiting() {
    return !pred_flag_is_granted();
  }
  inline bool pred_flag_is_granted() {
    return read_pred_flags() & kPredFlagGranted;
  }
  inline bool next_flag_is_direct_granted() {
    uint32_t f = (read_next_flags() & kSuccFlagMask);
    return f == kSuccFlagDirectGranted;
  }
  inline bool next_flag_is_leaving_granted() {
    uint32_t f = (read_next_flags() & kSuccFlagMask);
    return f == kSuccFlagLeavingGranted;
  }
  inline bool next_flag_is_granted() {
    uint32_t f = (read_next_flags() & kSuccFlagMask);
    return f == kSuccFlagLeavingGranted || f == kSuccFlagDirectGranted;
  }
  inline bool next_flag_is_leaving() {
    return (read_next_flags() & kSuccFlagMask) == kSuccFlagLeaving;
  }
  inline bool next_flag_is_waiting() {
    return (read_next_flags() & kSuccFlagMask) == kSuccFlagWaiting;
  }
  inline void set_next_flag_writer_successor() {
    ASSERT_ND(!next_flag_has_successor());
    assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
      &next_.components_.flags_, kSuccFlagSuccessorWriter);
  }
  inline void set_next_flag_reader_successor() {
    ASSERT_ND(!next_flag_has_successor());
    assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
      &next_.components_.flags_, kSuccFlagSuccessorReader);
  }
  inline void set_pred_flag_granted() {
    ASSERT_ND(pred_flag_is_waiting());
    assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
      &pred_.components_.flags_, kPredFlagGranted);
  }
  inline void set_next_flag_granted() {
    ASSERT_ND(pred_flag_is_granted());
    ASSERT_ND(next_flag_is_waiting() | next_flag_is_leaving());
    if (next_flag_is_waiting()) {
      assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
        &next_.components_.flags_, kSuccFlagDirectGranted);
    } else {
      ASSERT_ND(next_flag_is_leaving());
      assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
        &next_.components_.flags_, kSuccFlagLeavingGranted);
    }
  }
  inline void set_next_flag_busy_granted() {
    ASSERT_ND(pred_flag_is_granted());
    ASSERT_ND(next_flag_is_waiting() | next_flag_is_leaving());
    if (next_flag_is_waiting()) {
      assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
        &next_.components_.flags_, kSuccFlagDirectGranted | kSuccFlagBusy);
    } else {
      ASSERT_ND(next_flag_is_leaving());
      assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
        &next_.components_.flags_, kSuccFlagLeavingGranted | kSuccFlagBusy);
    }
  }
  inline void set_next_flag_leaving() {
    assorted::raw_atomic_exchange<uint16_t>(
      reinterpret_cast<uint16_t*>(&next_.components_.flags_),
      static_cast<uint16_t>(kSuccFlagLeaving));
  }
  inline void set_next_flag_no_successor() {
    assorted::raw_atomic_fetch_and_bitwise_and<uint32_t>(
      &next_.components_.flags_, ~kSuccFlagSuccessorClassMask);
  }
  inline void set_next(uint64_t next) {
    assorted::atomic_store_release<uint64_t>(&next_.data_, next);
  }
  inline bool cas_next_weak(uint64_t expected, uint64_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint64_t>(
     &next_.data_, &expected, desired);
  }
  inline bool cas_next_strong(uint64_t expected, uint64_t desired) {
    return assorted::raw_atomic_compare_exchange_strong<uint64_t>(
     &next_.data_, &expected, desired);
  }
  inline void set_flags_granted() {
    set_pred_flag_granted();
    set_next_flag_granted();
  }
  inline bool next_flag_has_successor() {
    return read_next_flags() & kSuccFlagSuccessorClassMask;
  }
  inline bool next_flag_has_reader_successor() {
    return (read_next_flags() & kSuccFlagSuccessorClassMask) == kSuccFlagSuccessorReader;
  }
  inline bool next_flag_has_writer_successor() {
    return (read_next_flags() & kSuccFlagSuccessorClassMask) == kSuccFlagSuccessorWriter;
  }
  inline bool next_flag_is_busy() {
    return (read_next_flags() & kSuccFlagBusy) == kSuccFlagBusy;
  }
  inline void set_next_flag_busy() {
    ASSERT_ND(!next_flag_is_busy());
    assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(
      &next_.components_.flags_, kSuccFlagBusy);
  }
  inline void unset_next_flag_busy() {
    ASSERT_ND(next_flag_is_busy());
    assorted::raw_atomic_fetch_and_bitwise_and<uint32_t>(
      &next_.components_.flags_, ~kSuccFlagBusy);
  }
  inline uint32_t cas_val_next_flag_strong(uint32_t expected, uint32_t desired) {
    assorted::raw_atomic_compare_exchange_strong<uint32_t>(
      &next_.components_.flags_, &expected, desired);
    return expected;
  }
  inline uint32_t cas_val_next_flag_weak(uint32_t expected, uint32_t desired) {
    assorted::raw_atomic_compare_exchange_weak<uint32_t>(
      &next_.components_.flags_, &expected, desired);
    return expected;
  }
  inline uint64_t cas_val_next_strong(uint64_t expected, uint64_t desired) {
    assorted::raw_atomic_compare_exchange_strong<uint64_t>(
      &next_.data_, &expected, desired);
    return expected;
  }
  inline uint64_t cas_val_next_weak(uint64_t expected, uint64_t desired) {
    assorted::raw_atomic_compare_exchange_weak<uint64_t>(
      &next_.data_, &expected, desired);
    return expected;
  }
  inline uint32_t xchg_next_id(uint32_t id) {
    return assorted::raw_atomic_exchange<uint32_t>(&next_.components_.id_, id);
  }
  inline bool cas_next_id_strong(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_strong<uint32_t>(
      &next_.components_.id_, &expected, desired);
  }
  inline bool cas_next_id_weak(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint32_t>(
      &next_.components_.id_, &expected, desired);
  }
  inline bool cas_pred_id_weak(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint32_t>(
      &pred_.components_.id_, &expected, desired);
  }
  inline bool cas_pred_id_strong(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_strong<uint32_t>(
      &pred_.components_.id_, &expected, desired);
  }
  inline uint32_t cas_val_pred_id_weak(uint32_t expected, uint32_t desired) {
    assorted::raw_atomic_compare_exchange_weak<uint32_t>(
      &pred_.components_.id_, &expected, desired);
    return expected;
  }
  inline uint32_t make_next_flag_waiting_with_no_successor() { return kSuccFlagWaiting; }
  inline uint32_t make_next_flag_waiting_with_reader_successor() {
    return kSuccFlagWaiting | kSuccFlagSuccessorReader;
  }
  inline uint32_t get_pred_id() {
    return assorted::atomic_load_acquire<uint32_t>(&pred_.components_.id_);
  }
  inline uint32_t get_next_id() {
    return assorted::atomic_load_acquire<uint32_t>(&next_.components_.id_);
  }
  inline uint64_t get_next() {
    return assorted::atomic_load_acquire<uint64_t>(&next_.data_);
  }
  inline void set_pred_id(uint32_t id) {
    assorted::atomic_store_release<uint32_t>(&pred_.components_.id_, id);
  }
  inline void set_next_id(uint32_t id) {
    assorted::atomic_store_release<uint32_t>(&next_.components_.id_, id);
  }
  inline uint32_t xchg_pred_id(uint32_t id) {
    return assorted::raw_atomic_exchange<uint32_t>(&pred_.components_.id_, id);
  }
  inline void init_reader() {
    pred_.components_.flags_ = kPredFlagReader;
    next_.components_.flags_ = 0;
    pred_.components_.id_ = next_.components_.id_ = 0;
    ASSERT_ND(pred_flag_is_waiting());
    ASSERT_ND(next_flag_is_waiting());
    ASSERT_ND(is_reader());
  }
  inline void init_writer() {
    pred_.components_.flags_ = kPredFlagWriter;
    next_.components_.flags_ = 0;
    pred_.components_.id_ = next_.components_.id_ = 0;
    ASSERT_ND(pred_flag_is_waiting());
    ASSERT_ND(next_flag_is_waiting());
    ASSERT_ND(is_writer());
  }
  bool timeout_granted(int32_t timeout);
};

/**
 * @brief An MCS reader-writer lock data structure.
 * @ingroup XCT
 * @details
 * This implements a fair reader-writer lock by the original authors of MCS lock [PPoPP 1991].
 * The version implemented here includes a bug fix due to Keir Fraser (University of Cambridge).
 * See https://www.cs.rochester.edu/research/synchronization/pseudocode/rw.html#s_f for
 * the original pseudocode with the fix.
 *
 * The major use case so far is row-level locking for 2PL.
 *
 * The assumption is that a thread at any instant can be **waiting** for only one MCS lock,
 * so knowing the thread ID suffices to locate the block index as well.
 *
 * TODO(tzwang): add the ownerless variant.
 */
struct McsRwLock {
  static const thread::ThreadId kNextWriterNone = 0xFFFFU;

  McsRwLock() { reset(); }

  McsRwLock(const McsRwLock& other) CXX11_FUNC_DELETE;
  McsRwLock& operator=(const McsRwLock& other) CXX11_FUNC_DELETE;

  inline void reset() {
    tail_ = nreaders_ = 0;
    set_next_writer(kNextWriterNone);
    assorted::memory_fence_release();
  }
  inline void increment_nreaders() {
    assorted::raw_atomic_fetch_add<uint16_t>(&nreaders_, 1);
  }
  inline uint16_t decrement_nreaders() {
    return assorted::raw_atomic_fetch_add<uint16_t>(&nreaders_, -1);
  }
  inline uint16_t nreaders() {
    return assorted::atomic_load_acquire<uint16_t>(&nreaders_);
  }
  inline McsBlockIndex get_tail_waiter_block() const { return tail_ & 0xFFFFU; }
  inline thread::ThreadId get_tail_waiter() const { return tail_ >> 16U; }
  inline bool has_next_writer() const {
    return assorted::atomic_load_acquire<thread::ThreadId>(&next_writer_) != kNextWriterNone;
  }
  inline void set_next_writer(thread::ThreadId thread_id) {
    xchg_next_writer(thread_id);  // sub-word access...
  }
  inline thread::ThreadId get_next_writer() {
    return assorted::atomic_load_acquire<thread::ThreadId>(&next_writer_);
  }
  inline thread::ThreadId xchg_next_writer(thread::ThreadId id) {
    return assorted::raw_atomic_exchange<thread::ThreadId>(&next_writer_, id);
  }
  bool cas_next_writer_weak(thread::ThreadId expected, thread::ThreadId desired) {
    return assorted::raw_atomic_compare_exchange_weak<thread::ThreadId>(
      &next_writer_, &expected, desired);
  }
  bool cas_next_writer_strong(thread::ThreadId expected, thread::ThreadId desired) {
    return assorted::raw_atomic_compare_exchange_strong<thread::ThreadId>(
      &next_writer_, &expected, desired);
  }
  inline uint32_t xchg_tail(uint32_t new_tail) {
    return assorted::raw_atomic_exchange<uint32_t>(&tail_, new_tail);
  }
  inline bool cas_tail_strong(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_strong<uint32_t>(&tail_, &expected, desired);
  }
  inline bool cas_tail_weak(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint32_t>(&tail_, &expected, desired);
  }
  static inline uint32_t to_tail_int(
    thread::ThreadId tail_waiter,
    McsBlockIndex tail_waiter_block) {
    ASSERT_ND(tail_waiter_block <= 0xFFFFU);
    return static_cast<uint32_t>(tail_waiter) << 16 | (tail_waiter_block & 0xFFFFU);
  }
  inline uint32_t get_tail_int() {
    return assorted::atomic_load_acquire<uint32_t>(&tail_);
  }
  bool is_locked() const {
    return (tail_ & 0xFFFFU) != 0 || nreaders_ > 0;
  }

  uint32_t tail_;                 // +4 => 4
  /* Note that threadId starts from 0, so we use 0xFFFF as the "invalid"
   * marker, unless we make the lock even larger than 8 bytes. This essentially
   * limits the largest allowed number of cores we support to 256 sockets x 256
   * cores per socket - 1.
   */
  thread::ThreadId next_writer_;  // +2 => 6
  uint16_t nreaders_;             // +2 => 8

  friend std::ostream& operator<<(std::ostream& o, const McsRwLock& v);
};

struct McsRwAsyncMapping {
  UniversalLockId lock_id_;
  McsBlockIndex block_index_;
  char padding_[16 - sizeof(McsBlockIndex) - sizeof(UniversalLockId)];

  McsRwAsyncMapping(UniversalLockId lock_id, McsBlockIndex block) :
    lock_id_(lock_id), block_index_(block) {}
  McsRwAsyncMapping() : lock_id_(kNullUniversalLockId), block_index_(0) {}
};

const uint64_t kXctIdDeletedBit     = 1ULL << 63;
const uint64_t kXctIdMovedBit       = 1ULL << 62;
const uint64_t kXctIdBeingWrittenBit = 1ULL << 61;
const uint64_t kXctIdNextLayerBit    = 1ULL << 60;
const uint64_t kXctIdMaskSerializer = 0x0FFFFFFFFFFFFFFFULL;
const uint64_t kXctIdMaskEpoch      = 0x0FFFFFFF00000000ULL;
const uint64_t kXctIdMaskOrdinal    = 0x00000000FFFFFFFFULL;

/**
 * @brief Maximum value of in-epoch ordinal.
 * @ingroup XCT
 * @details
 * We reserve 4 bytes in XctId, but in reality 3 bytes are more than enough.
 * By restricting it to within 3 bytes, we can pack more information in a few places.
 */
const uint64_t kMaxXctOrdinal       = (1ULL << 24) - 1U;

/**
 * @brief Persistent status part of Transaction ID
 * @ingroup XCT
 * @details
 * Unlike what [TU13] Sec 4.2 defines, FOEDUS's TID is 128 bit to contain more information.
 * XctId represents a half (64bit) of TID that is used to represent persistent status of the record,
 * such as record versions. The locking-mechanism part is separated to another half; McsLock.
 *
 * @par Bit Assignments
 * <table>
 * <tr><th>Bits</th><th>Name</th><th>Description</th></tr>
 * <tr><td>1</td><td>Psuedo-delete bit</td><td>Whether te key is logically non-existent.</td></tr>
 * <tr><td>2</td><td>Moved bit</td><td>This is used for the Master-tree foster-twin protocol.
 * when a record is moved from one page to another during split.</td></tr>
 * <tr><td>3</td><td>BeingWritten</td><td>Before we start applying modifications to a record,
 * we set true to this so that optimistic-read can easily check for half-updated value.
 * After the modification, we set false to this. Of course with appropriate fences.</td></tr>
 * <tr><td>4</td><td>NextLayer</td><td>This is used only in Masstree. This bit indicates whether
 * the record represents a pointer to next layer. False if it is a tuple itself. We put this
 * information as part of XctId because we sometimes have to transactionally know whether the
 * record is a next-layer pointer or not. There is something wrong if a read-set or write-set
 * contains an XctId whose NextLayer bit is ON, because then the record is not a logical tuple.
 * In other words, a reading transaction can efficiently protect their reads on a record that
 * might become a next-layer pointer with a simple check after the usual read protocol.</td></tr>
 * <tr><td>5..32</td><td>Epoch</td><td>The recent owning transaction was in this Epoch.
 * We don't consume full 32 bits for epoch.
 * Assuming 20ms per epoch, 28bit still represents 1 year. All epochs will be refreshed by then
 * or we can have some periodic mantainance job to make it sure.</td></tr>
 * <tr><td>33..64</td><td>Ordinal</td><td>The recent owning transaction had this ordinal
 * in the epoch. We assign 32 bits. Thus we no longer have the case where we have to
 * increment current epoch even when there are many dependencies between transactions.
 * We still have the machanism to do so, but in reality it won't be triggered.
 * </td></tr>
 * </table>
 *
 * @par Greater than/Less than as 64-bit integer
 * The last 60 bits represent the serialization order of the transaction. Sometimes not exactly
 * the chronological order, but enough to assure serializability, see discussion in Sec 4.2 of
 * [TU13]. This class thus provides before() method to check \e strict order of
 * two instantances. Be aware of the following things, though:
 *  \li Epoch might be invalid/uninitialized (zero). An invalid epoch is \e before everything else.
 *  \li Epoch might wrap-around. We use the same wrap-around handling as foedus::Epoch.
 *  \li Ordinal is not a strict ordinal unless there is a dependency between transactions
 * in different cores. In that case, commit protocol adjusts the ordinal for serializability.
 * See [TU13] or their code (gen_commit_tid() in proto2_impl.h).
 *  \li We can \e NOT provide "equals" semantics via simple integer comparison. 61th- bits are
 * status bits, thus we have to mask it. equals_serial_order() does it.
 *
 * @par No Thread-ID
 * This is one difference from SILO. FOEDUS's XctID does not store thread-ID of last commit.
 * We don't use it for any purpose.
 *
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctId {
  XctId() : data_(0) {}

  void set(Epoch::EpochInteger epoch_int, uint32_t ordinal) {
    ASSERT_ND(epoch_int < Epoch::kEpochIntOverflow);
    ASSERT_ND(ordinal <= kMaxXctOrdinal);
    data_ = static_cast<uint64_t>(epoch_int) << 32 | ordinal;
  }

  Epoch   get_epoch() const ALWAYS_INLINE { return Epoch(get_epoch_int()); }
  void    set_epoch(Epoch epoch) ALWAYS_INLINE { set_epoch_int(epoch.value()); }
  Epoch::EpochInteger get_epoch_int() const ALWAYS_INLINE {
    return (data_ & kXctIdMaskEpoch) >> 32;
  }
  void    set_epoch_int(Epoch::EpochInteger epoch_int) ALWAYS_INLINE {
    ASSERT_ND(epoch_int < Epoch::kEpochIntOverflow);
    data_ = (data_ & ~kXctIdMaskEpoch) | (static_cast<uint64_t>(epoch_int) << 32);
  }
  bool    is_valid() const ALWAYS_INLINE { return get_epoch_int() != Epoch::kEpochInvalid; }


  uint32_t  get_ordinal() const ALWAYS_INLINE {
    ASSERT_ND(static_cast<uint32_t>(data_) <= kMaxXctOrdinal);
    return static_cast<uint32_t>(data_);
  }
  void      set_ordinal(uint32_t ordinal) ALWAYS_INLINE {
    ASSERT_ND(ordinal <= kMaxXctOrdinal);
    data_ = (data_ & (~kXctIdMaskOrdinal)) | ordinal;
  }
  void      increment_ordinal() ALWAYS_INLINE {
    uint32_t ordinal = get_ordinal();
    set_ordinal(ordinal + 1U);
  }
  /**
   * Returns -1, 0, 1 when this is less than, same, larger than other in terms of epoch/ordinal
   * @pre this->is_valid(), other.is_valid()
   * @pre this->get_ordinal() != 0, other.get_ordinal() != 0
   */
  int       compare_epoch_and_orginal(const XctId& other) const ALWAYS_INLINE {
    // compare epoch
    if (get_epoch_int() != other.get_epoch_int()) {
      Epoch this_epoch = get_epoch();
      Epoch other_epoch = other.get_epoch();
      ASSERT_ND(this_epoch.is_valid());
      ASSERT_ND(other_epoch.is_valid());
      if (this_epoch < other_epoch) {
        return -1;
      } else {
        ASSERT_ND(this_epoch > other_epoch);
        return 1;
      }
    }

    // if the epoch is the same, compare in_epoch_ordinal_.
    ASSERT_ND(get_epoch() == other.get_epoch());
    if (get_ordinal() < other.get_ordinal()) {
      return -1;
    } else if (get_ordinal() > other.get_ordinal()) {
      return 1;
    } else {
      return 0;
    }
  }

  void    set_being_written() ALWAYS_INLINE { data_ |= kXctIdBeingWrittenBit; }
  void    set_write_complete() ALWAYS_INLINE { data_ &= (~kXctIdBeingWrittenBit); }
  /**
   * Returns a version of this Xid whose being_written flag is off.
   * This internally spins until the bit becomes false, so use it only where deadlock
   * is not possible.
   */
  XctId   spin_while_being_written() const ALWAYS_INLINE;
  void    set_deleted() ALWAYS_INLINE { data_ |= kXctIdDeletedBit; }
  void    set_notdeleted() ALWAYS_INLINE { data_ &= (~kXctIdDeletedBit); }
  void    set_moved() ALWAYS_INLINE { data_ |= kXctIdMovedBit; }
  void    set_next_layer() ALWAYS_INLINE {
    // Delete-bit has no meaning for a next-layer record. To avoid confusion, turn it off.
    data_ = (data_ & (~kXctIdDeletedBit)) | kXctIdNextLayerBit;
  }
  // note, we should not need this method because becoming a next-layer-pointer is permanent.
  // we never revert it, which simplifies a concurrency control.
  // void    set_not_next_layer() ALWAYS_INLINE { data_ &= (~kXctIdNextLayerBit); }

  bool    is_being_written() const ALWAYS_INLINE {
    return (assorted::atomic_load_acquire<uint64_t>(&data_) & kXctIdBeingWrittenBit) != 0; }
  bool    is_deleted() const ALWAYS_INLINE { return (data_ & kXctIdDeletedBit) != 0; }
  bool    is_moved() const ALWAYS_INLINE { return (data_ & kXctIdMovedBit) != 0; }
  bool    is_next_layer() const ALWAYS_INLINE { return (data_ & kXctIdNextLayerBit) != 0; }
  /** is_moved() || is_next_layer() */
  bool    needs_track_moved() const ALWAYS_INLINE {
    return (data_ & (kXctIdMovedBit | kXctIdNextLayerBit)) != 0;
  }


  bool operator==(const XctId &other) const ALWAYS_INLINE { return data_ == other.data_; }
  bool operator!=(const XctId &other) const ALWAYS_INLINE { return data_ != other.data_; }

  /**
   * @brief Kind of std::max(this, other).
   * @details
   * This relies on the semantics of before(). Thus, this can't differentiate two XctId that
   * differ only in status bits. This method is only used for XctId generation at commit time,
   * so that's fine.
   */
  void store_max(const XctId& other) ALWAYS_INLINE {
    if (!other.is_valid()) {
      return;
    }

    if (before(other)) {
      operator=(other);
    }
  }

  /**
   * Returns if this XctId is \e before other in serialization order, meaning this is either an
   * invalid (unused) epoch or strictly less than the other.
   * @pre other.is_valid()
   */
  bool before(const XctId &other) const ALWAYS_INLINE {
    ASSERT_ND(other.is_valid());
    // compare epoch, then ordinal
    if (get_epoch_int() != other.get_epoch_int()) {
      return get_epoch().before(other.get_epoch());
    }
    return get_ordinal() < other.get_ordinal();
  }

  void clear_status_bits() { data_ &= kXctIdMaskSerializer; }

  friend std::ostream& operator<<(std::ostream& o, const XctId& v);

  uint64_t            data_;
};

/**
 * @brief Transaction ID, a 128-bit data to manage record versions and provide locking mechanism.
 * @ingroup XCT
 * @details
 * This object contains a quite more information compared to SILO [TU13]'s TID.
 * We spend more bits on ordinals and epochs for larger environments, and also employ MCS-locking
 * to be more scalable. Thus, now it's 128-bits.
 * It's not a negligible size, but still compact. Also, 16-bytes sometimes reduce false cacheline
 * sharing (well, then you might ask making it 64 bytes... but that's too much).
 *
 * @par McsLock and XctId
 * McsLock provides the locking mechanism, namely MCS locking.
 * XctId provides the record version information protected by the lock.
 *
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct LockableXctId {
  /** the first 64bit: Locking part of TID */
  McsLock       lock_;
  /** the second 64bit: Persistent status part of TID. */
  XctId         xct_id_;

  McsLock* get_key_lock() ALWAYS_INLINE { return &lock_; }
  bool is_keylocked() const ALWAYS_INLINE { return lock_.is_locked(); }
  bool is_deleted() const ALWAYS_INLINE { return xct_id_.is_deleted(); }
  bool is_moved() const ALWAYS_INLINE { return xct_id_.is_moved(); }
  bool is_next_layer() const ALWAYS_INLINE { return xct_id_.is_next_layer(); }
  bool needs_track_moved() const ALWAYS_INLINE { return xct_id_.needs_track_moved(); }
  bool is_being_written() const ALWAYS_INLINE { return xct_id_.is_being_written(); }

  /** used only while page initialization */
  void    reset() ALWAYS_INLINE {
    lock_.reset();
    xct_id_.data_ = 0;
  }
  friend std::ostream& operator<<(std::ostream& o, const LockableXctId& v);
};

/**
 * @brief The MCS reader-writer lock variant of LockableXctId.
 */
struct RwLockableXctId {
  /** the first 64bit: Locking part of TID */
  McsRwLock       lock_;

  /** the second 64bit: Persistent status part of TID. */
  XctId         xct_id_;

  McsRwLock* get_key_lock() ALWAYS_INLINE { return &lock_; }
  bool is_keylocked() const ALWAYS_INLINE { return lock_.is_locked(); }
  bool is_deleted() const ALWAYS_INLINE { return xct_id_.is_deleted(); }
  bool is_moved() const ALWAYS_INLINE { return xct_id_.is_moved(); }
  bool is_next_layer() const ALWAYS_INLINE { return xct_id_.is_next_layer(); }
  bool needs_track_moved() const ALWAYS_INLINE { return xct_id_.needs_track_moved(); }
  bool is_being_written() const ALWAYS_INLINE { return xct_id_.is_being_written(); }
  bool is_hot(thread::Thread* context) const;
  void hotter(thread::Thread* context) const;

  /** used only while page initialization */
  void    reset() ALWAYS_INLINE {
    lock_.reset();
    xct_id_.data_ = 0;
  }
  friend std::ostream& operator<<(std::ostream& o, const RwLockableXctId& v);
};

/**
 * @brief Auto-release object for WW MCS locking.
 * @ingroup XCT
 * @details
 * This is so far used only in page-locking during physical structual operations (SMO).
 * We always lock only one public (excluding private pages that are yet to be read by others)
 * page at a time, so always unconditional lock without worry on deadlocks.
 */
struct McsLockScope {
  McsLockScope();
  McsLockScope(
    thread::Thread* context,
    LockableXctId* lock,
    bool acquire_now = true,
    bool non_racy_acquire = false);
  McsLockScope(
    thread::Thread* context,
    McsLock* lock,
    bool acquire_now = true,
    bool non_racy_acquire = false);
  ~McsLockScope();

  /// scope object is movable, but not copiable.
  McsLockScope(const McsLockScope& other) CXX11_FUNC_DELETE;
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  McsLockScope(McsLockScope&& other);
  McsLockScope& operator=(McsLockScope&& other);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

  void initialize(thread::Thread* context, McsLock* lock, bool acquire_now, bool non_racy_acquire);

  bool is_valid() const { return lock_; }
  bool is_locked() const { return block_ != 0; }

  /** Acquires the lock. Does nothing if already acquired or !is_valid(). */
  void acquire(bool non_racy_acquire);
  /** Release the lock if acquired. Does nothing if not or !is_valid(). */
  void release();

  /** Just for PageVersionLockScope(McsLockScope*) */
  void move_to(storage::PageVersionLockScope* new_owner);

 private:
  thread::Thread* context_;
  McsLock*        lock_;
  /** Non-0 when locked. 0 when already released or not yet acquired. */
  McsBlockIndex   block_;
};

/**
 * @brief Auto-release object for WW MCS locking.
 * @ingroup XCT
 * @details
 * This is used in a few record-locking during physical structual operations (SMO).
 * Because this lock is unconditional, we have to be careful on risk of deadlocks when we do that.
 * We recommend using the "try" version whenever possible, and writing a safe code
 * that does appropriate handling if the try fails.
 */
struct McsRwLockScope {
  explicit McsRwLockScope(bool as_reader);
  McsRwLockScope(
    thread::Thread* context,
    RwLockableXctId* lock,
    bool as_reader,
    bool acquire_now,
    bool is_try_acquire);
  McsRwLockScope(
    thread::Thread* context,
    McsRwLock* lock,
    bool as_reader,
    bool acquire_now,
    bool is_try_acquire);
  ~McsRwLockScope();

  /// scope object is movable, but not copiable.
  McsRwLockScope(const McsRwLockScope& other) CXX11_FUNC_DELETE;
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  McsRwLockScope(McsRwLockScope&& other);
  McsRwLockScope& operator=(McsRwLockScope&& other);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

  void initialize(
    thread::Thread* context,
    McsRwLock* lock,
    bool as_reader,
    bool acquire_now,
    bool is_try_acquire);

  bool is_valid() const { return lock_; }
  bool is_locked() const { return block_ != 0; }

  /** Embodies both of the followings. @return whether we acquired or not */
  bool acquire_general(bool is_try_acquire);
  /** Unconditionally acquires. Does nothing if already acquired or !is_valid(). */
  void unconditional_acquire();
  /** Instanteneously try to acquire. @return whether we acquired or not */
  bool try_acquire();

  /** Release the lock if acquired. Does nothing if not or !is_valid(). */
  void release();

 private:
  thread::Thread* context_;
  McsRwLock*      lock_;
  /** Non-0 when locked. 0 when already released or not yet acquired. */
  McsBlockIndex   block_;
  bool            as_reader_;
};

class McsOwnerlessLockScope {
 public:
  McsOwnerlessLockScope();
  McsOwnerlessLockScope(
    McsLock* lock,
    bool acquire_now = true,
    bool non_racy_acquire = false);
  ~McsOwnerlessLockScope();

  bool is_valid() const { return lock_; }
  bool is_locked_by_me() const { return locked_by_me_; }

  /** Acquires the lock. Does nothing if already acquired or !is_valid(). */
  void acquire(bool non_racy_acquire);
  /** Release the lock if acquired. Does nothing if not or !is_valid(). */
  void release();

 private:
  McsLock*        lock_;
  bool            locked_by_me_;
};

/** Result of track_moved_record(). When failed to track, both null. */
struct TrackMovedRecordResult {
  TrackMovedRecordResult()
    : new_owner_address_(CXX11_NULLPTR), new_payload_address_(CXX11_NULLPTR) {}
  TrackMovedRecordResult(RwLockableXctId* new_owner_address, char* new_payload_address)
    : new_owner_address_(new_owner_address), new_payload_address_(new_payload_address) {}

  RwLockableXctId* new_owner_address_;
  char* new_payload_address_;
};

inline XctId XctId::spin_while_being_written() const {
  uint64_t copied_data = assorted::atomic_load_acquire<uint64_t>(&data_);
  if (UNLIKELY(copied_data & kXctIdBeingWrittenBit)) {
    while (copied_data & kXctIdBeingWrittenBit) {
      copied_data = assorted::atomic_load_acquire<uint64_t>(&data_);
    }
  }
  XctId ret;
  ret.data_ = copied_data;
  return ret;
}


/**
 * Always use this method rather than doing the conversion yourself.
 * @see UniversalLockId
 */
UniversalLockId to_universal_lock_id(
  const memory::GlobalVolatilePageResolver& resolver,
  uintptr_t lock_ptr);
// just shorthands.
inline UniversalLockId xct_id_to_universal_lock_id(
  const memory::GlobalVolatilePageResolver& resolver,
  RwLockableXctId* lock) {
  return to_universal_lock_id(resolver, reinterpret_cast<uintptr_t>(lock));
}
inline UniversalLockId rw_lock_to_universal_lock_id(
  const memory::GlobalVolatilePageResolver& resolver,
  McsRwLock* lock) {
  return to_universal_lock_id(resolver, reinterpret_cast<uintptr_t>(lock));
}

/**
 * Always use this method rather than doing the conversion yourself.
 * @see UniversalLockId
 */
RwLockableXctId* from_universal_lock_id(
  const memory::GlobalVolatilePageResolver& resolver,
  const UniversalLockId universal_lock_id);

// sizeof(XctId) must be 64 bits.
STATIC_SIZE_CHECK(sizeof(XctId), sizeof(uint64_t))
STATIC_SIZE_CHECK(sizeof(McsLock), 8)
STATIC_SIZE_CHECK(sizeof(LockableXctId), 16)

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
