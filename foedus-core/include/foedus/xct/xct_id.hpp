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

/** Index in thread-local MCS block. 0 means not locked. */
typedef uint32_t McsBlockIndex;

/**
 * @brief Pre-allocated MCS block (node). we so far pre-allocate at most 2^16 nodes per thread.
 * @ingroup XCT
 * @details
 * So far this is an 8-byte struct. We for some reason observed a huge performance drop (60%)
 * after we increased this to 16 bytes whenever there is a lock contention.
 * Need to figure out why.
 */
struct McsBlock {
  /**
   * Whether this thread is waiting for some other lock owner.
   * While this is true, the thread spins on this \e local variable.
   * The lock owner updates this when it unlocks.
   */
  bool      waiting_;             // +1 -> 1
  /** just for sanity check. last 1 byte of the MCS lock's address */
  uint8_t   lock_addr_tag_;       // +1 -> 2

  thread::ThreadId  successor_;         // +2 -> 4
  McsBlockIndex     successor_block_;   // +4 -> 8

  inline bool             has_successor() const ALWAYS_INLINE {
    return successor_block_ != 0;
  }
  inline thread::ThreadId get_successor_thread_id() const ALWAYS_INLINE {
    return successor_;
  }
  inline McsBlockIndex    get_successor_block() const ALWAYS_INLINE {
    return successor_block_;
  }
  inline void clear_successor() ALWAYS_INLINE {
    successor_ = 0;
    successor_block_ = 0;
  }
  inline void set_successor(thread::ThreadId thread_id, McsBlockIndex block) ALWAYS_INLINE {
    // we must make sure we set successor_ before successor_block_. It's the contract.
    // If these two writes are in one-shot, we don't have to worry about it.
    successor_ = thread_id;
    assorted::memory_fence_release();
    successor_block_ = block;
  }

  /// The code below makes it a 16-byte struct. So far disabled, but will use this in near future.
  // char      unused_[6];           // +6 -> 8

  /**
   * The successor of MCS lock queue after this thread (in other words, the thread that is
   * waiting for this thread). Successor is represented by thread ID and block,
   * the index in mcs_blocks_. This member's high-32 bits are thread ID, low-32 bits are block.
   * The two must be set in one-shot.
   */
  // uint64_t  successor_combined_;  // +8 -> 16

  /// setter/getter for successor_combined_.
  /*
  inline bool             has_successor() const ALWAYS_INLINE {
    return successor_combined_ != 0;
  }
  inline thread::ThreadId get_successor_thread_id() const ALWAYS_INLINE {
    return successor_combined_ >> 32;
  }
  inline McsBlockIndex    get_successor_block() const ALWAYS_INLINE {
    return successor_combined_ & 0xFFFFFFFFU;
   }
  inline void clear_successor() ALWAYS_INLINE { successor_combined_ = 0; }
  inline void set_successor(thread::ThreadId thread_id, McsBlockIndex block) ALWAYS_INLINE {
    uint64_t new_value = thread_id;
    new_value <<= 32;
    new_value |= block;
    // Install it in always one shot.
    *(&successor_combined_) = new_value;
  }
  */
};


/**
 * @brief An MCS lock data structure.
 * @ingroup XCT
 * @details
 * This is the minimal unit of locking in our system.
 * Unlike SILO, we employ MCS locking that scales much better on big machines.
 * This object stores \e tail-waiter, which indicates the thread that is in the tail of the queue
 * lock, which \e might be the owner of the lock.
 * The MCS-lock nodes are pre-allocated for each thread and placed in shared memory.
 */
struct McsLock {
  McsLock() { data_ = 0; }
  McsLock(thread::ThreadId tail_waiter, McsBlockIndex tail_waiter_block) {
    reset(tail_waiter, tail_waiter_block);
  }

  McsLock(const McsLock& other) CXX11_FUNC_DELETE;
  McsLock& operator=(const McsLock& other) CXX11_FUNC_DELETE;

  /** Used only for sanity check */
  uint8_t   last_1byte_addr() const ALWAYS_INLINE {
    // address is surely a multiply of 4. omit that part.
    ASSERT_ND(reinterpret_cast<uintptr_t>(reinterpret_cast<const void*>(this)) % 4 == 0);
    return reinterpret_cast<uintptr_t>(reinterpret_cast<const void*>(this)) / 4;
  }
  bool      is_locked() const { return (data_ & 0xFFFFU) != 0; }

  /** Equivalent to context->mcs_acquire_lock(this). Actually that's more preferred. */
  McsBlockIndex acquire_lock(thread::Thread* context);
  /** This doesn't use any atomic operation to take a lock. only allowed when there is no race */
  McsBlockIndex initial_lock(thread::Thread* context);
  /** Equivalent to context->mcs_release_lock(this). Actually that's more preferred. */
  void          release_lock(thread::Thread* context, McsBlockIndex block);

  thread::ThreadId get_tail_waiter() const ALWAYS_INLINE { return data_ >> 16U; }
  McsBlockIndex get_tail_waiter_block() const ALWAYS_INLINE { return data_ & 0xFFFFU; }

  /** used only while page initialization */
  void    reset() ALWAYS_INLINE { data_ = 0; }

  /** used only for initial_lock() */
  void    reset(thread::ThreadId tail_waiter, McsBlockIndex tail_waiter_block) ALWAYS_INLINE {
    data_ = to_int(tail_waiter, tail_waiter_block);
  }

  static uint32_t to_int(
    thread::ThreadId tail_waiter,
    McsBlockIndex tail_waiter_block) ALWAYS_INLINE {
    ASSERT_ND(tail_waiter_block <= 0xFFFFU);
    return static_cast<uint32_t>(tail_waiter) << 16 | (tail_waiter_block & 0xFFFFU);
  }

  friend std::ostream& operator<<(std::ostream& o, const McsLock& v);

  uint32_t data_;
};

/**
 * @brief MCS lock for key lock combined with other status; flags and range locks.
 * @ingroup XCT
 * @details
 * @par Range Lock
 * Unlike SILO [TU13], we use range-lock bit for protecting a gap rather than a node set, which
 * is unnecessarily conservative. It basically works same as key lock. One thing to remember is that
 * each B-tree page has an inclusive low-fence key and an exclusive high-fence key.
 * Range lock can protect a region from low-fence to the first key and a region from last key to
 * high-fence key.
 */
class CombinedLock {
 public:
  enum Constants {
    kMaskVersion  = 0xFFFF,
    // 17th-24th bits reserved for range lock implementation
    kRangelockBit = 1 << 17,
  };

  CombinedLock() : key_lock_(), other_locks_(0) {}

  bool is_keylocked() const ALWAYS_INLINE { return key_lock_.is_locked(); }
  bool is_rangelocked() const ALWAYS_INLINE { return (other_locks_ & kRangelockBit) != 0; }

  McsLock* get_key_lock() ALWAYS_INLINE { return &key_lock_; }
  const McsLock* get_key_lock() const ALWAYS_INLINE { return &key_lock_; }

  /** Not used yet... */
  /*
   * Highest 1 byte represents a loosely maintained NUMA-node of the last locker.
   * This is only used as statistics in partitioning. Not atomically updated.
   */
  // uint8_t get_node_stat() const { return (other_locks_ & 0xFF000000U) >> 24;}
  // void set_node_stat(uint8_t node) {
  //  other_locks_ = (other_locks_ & (0x00FFFFFFU)) | (static_cast<uint32_t>(node) << 24);
  // }

  uint16_t get_version() const { return other_locks_ & kMaskVersion; }
  void increment_version() {
    ASSERT_ND(is_keylocked());
    uint16_t version = get_version() + 1;
    other_locks_ = (other_locks_ & (~kMaskVersion)) | version;
  }

  /** used only while page initialization */
  void    reset() ALWAYS_INLINE {
    key_lock_.reset();
    other_locks_ = 0;
  }

  friend std::ostream& operator<<(std::ostream& o, const CombinedLock& v);

 private:
  McsLock   key_lock_;
  uint32_t  other_locks_;
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
 * such as record versions. The locking-mechanism part is separated to another half; CombinedLock.
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

  bool    is_being_written() const ALWAYS_INLINE { return (data_ & kXctIdBeingWrittenBit) != 0; }
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
 * @par CombinedLock and XctId
 * CombinedLock provides the locking mechanism, namely MCS locking.
 * XctId provides the record version information protected by the lock.
 *
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct LockableXctId {
  /** the first 64bit: Locking part of TID */
  CombinedLock  lock_;
  /** the second 64bit: Persistent status part of TID. */
  XctId         xct_id_;

  McsLock* get_key_lock() ALWAYS_INLINE { return lock_.get_key_lock(); }
  bool is_keylocked() const ALWAYS_INLINE { return lock_.is_keylocked(); }
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
 * @brief Auto-release object for MCS locking.
 * @ingroup XCT
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

/** Result of track_moved_record(). When failed to track, both null. */
struct TrackMovedRecordResult {
  TrackMovedRecordResult()
    : new_owner_address_(CXX11_NULLPTR), new_payload_address_(CXX11_NULLPTR) {}
  TrackMovedRecordResult(LockableXctId* new_owner_address, char* new_payload_address)
    : new_owner_address_(new_owner_address), new_payload_address_(new_payload_address) {}

  LockableXctId* new_owner_address_;
  char* new_payload_address_;
};


// sizeof(XctId) must be 64 bits.
STATIC_SIZE_CHECK(sizeof(XctId), sizeof(uint64_t))
STATIC_SIZE_CHECK(sizeof(CombinedLock), 8)
STATIC_SIZE_CHECK(sizeof(McsLock), 4)
STATIC_SIZE_CHECK(sizeof(LockableXctId), 16)

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
