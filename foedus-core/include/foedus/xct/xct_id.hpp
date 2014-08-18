/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
 */
enum IsolationLevel {
  /**
   * No guarantee at all for reads, for the sake of best performance and scalability.
   * This avoids checking and even storing read set, thus provides the best performance.
   * However, concurrent transactions might be modifying the data the transaction is now reading.
   * So, this has a chance of reading half-changed data.
   * To ameriolate the issue a bit, this mode prefers snapshot pages if both a snapshot page
   * and a volatile page is available. In other words, more consistent but more stale data.
   */
  kDirtyReadPreferSnapshot,

  /**
   * Basically same as kDirtyReadPreferSnapshot, but this mode prefers volatile pages
   * if both a snapshot page and a volatile page is available. In other words,
   * more recent but more inconsistent data.
   */
  kDirtyReadPreferVolatile,

  /**
   * Snapshot isolation, meaning the transaction might see or be based on stale snapshot.
   * Optionally, the client can specify which snapshot we should be based on.
   */
  kSnapshot,

  /**
   * Protects against all anomalies in all situations.
   * This is the most expensive level, but everything good has a price.
   */
  kSerializable,
};

/** Index in thread-local MCS block. 0 means not locked. */
typedef uint32_t McsBlockIndex;

/**
 * @brief An MCS lock data structure.
 * @details
 * This is the minimal unit of locking in our system.
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

 private:
  /** not used. GCC gives a completely weird behavior even with fno-strict-aliasing. */
  union McsLockData {
    struct Components {
      /** last waiter's thread ID */
      thread::ThreadId  tail_waiter_;
      /** last waiter's MCS block in the thread. 0 means no one is waiting (not locked) */
      uint16_t          tail_waiter_block_;  // so far 16bits, so not McsBlockIndex
    } components;
    uint32_t word;
  };
};
STATIC_SIZE_CHECK(sizeof(McsLock), 4)

/**
 * MCS lock for key lock combined with other status; flags and range locks.
 */
class CombinedLock {
 public:
  enum Constants {
    kMaskVersion  = 0xFFFF,
    kRangelockBit = 1 << 17,
    // more bits reserved for range lock implementation
  };

  CombinedLock() : key_lock_(), other_locks_(0) {}

  bool is_keylocked() const ALWAYS_INLINE { return key_lock_.is_locked(); }
  bool is_rangelocked() const ALWAYS_INLINE { return (other_locks_ & kRangelockBit) != 0; }

  McsLock* get_key_lock() ALWAYS_INLINE { return &key_lock_; }
  const McsLock* get_key_lock() const ALWAYS_INLINE { return &key_lock_; }

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
STATIC_SIZE_CHECK(sizeof(CombinedLock), 8)

const uint64_t kXctIdDeletedBit     = 1ULL << 63;
const uint64_t kXctIdMovedBit       = 1ULL << 62;
const uint64_t kXctIdBeingWrittenBit = 1ULL << 61;
const uint64_t kXctIdReservedBit    = 1ULL << 60;
const uint64_t kXctIdMaskSerializer = 0x0FFFFFFFFFFFFFFFULL;
const uint64_t kXctIdMaskEpoch      = 0x0FFFFFFF00000000ULL;
const uint64_t kXctIdMaskOrdinal    = 0x00000000FFFFFFFFULL;

/**
 * TODO(Hideaki) needs to overhaul the comment. now it's 128 bits.
 * @brief Transaction ID, a 64-bit data to identify transactions and record versions.
 * @ingroup XCT
 * @details
 * This object is basically equivalent to what [TU13] Sec 4.2 defines.
 * The difference is described below.
 *
 * @par Bit Assignments
 * <table>
 * <tr><th>Bits</th><th>Name</th><th>Description</th></tr>
 * <tr><td>1..28</td><td>Epoch</td><td>The recent owning transaction was in this Epoch.
 * We don't consume full 32 bits for epoch.
 * Assuming 20ms per epoch, 28bit still represents 1 year. All epochs will be refreshed by then
 * or we can have some periodic mantainance job to make it sure.</td></tr>
 * <tr><td>29..44</td><td>Ordinal</td><td>The recent owning transaction had this ordinal
 * in the epoch. We assign 16 bits. Thus 64k xcts per epoch.
 * A short transaction might exceed it, but then it can just increment current epoch.
 * Also, if there are no dependencies between transactions on each core, it could be
 * up to 64k xcts per epoch per core. See commit protocol.
 * </td></tr>
 * <tr><td>45</td><td>Reserved</td><td>Reserved for later use.</td></tr>
 * <tr><td>46</td><td>Range Lock bit</td><td>Lock the interval from the key to next key.</td></tr>
 * <tr><td>47</td><td>Psuedo-delete bit</td><td>Logically delete the key.</td></tr>
 * <tr><td>48</td><td>Moved bit</td><td>This is used for the Master-tree foster-twin protocol.
 * when a record is moved from one page to another during split.</td></tr>
 * <tr><td>49</td><td>Key Lock bit</td><td>Lock the key. This and the next tail waiter
 * forms its own 16bit region, which is atomically swapped in MCS locking.</td></tr>
 * <tr><td>50..64</td><td>Tail Waiter for MCS locking</td><td>
 * Unlike SILO, we use this thread ID for MCS locking that scales much better on big machines.
 * This indicates the thread that is in the tail of the queue lock, which might be the owner
 * of the lock.
 * Also, we spend only 15bits here to make this and lockbit a 16bits region.
 * At most 128 cores per NUMA node (50-57 NUMA node, 58-64 core).
 * </td></tr>
 * </table>
 *
 * @par Greater than/Less than as 64-bit integer
 * The first 60 bits represent the serialization order of the transaction. Sometimes not exactly
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
 * @par Range Lock
 * Unlike Sile [TU13], we use range-lock bit for protecting a gap rather than a node set, which
 * is unnecessarily conservative. It basically works same as key lock. One thing to remember is that
 * each B-tree page has an inclusive low-fence key and an exclusive high-fence key.
 * Range lock can protect a region from low-fence to the first key and a region from last key to
 * high-fence key.
 *
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctId {
  XctId() : data_(0) {}

  void set(Epoch::EpochInteger epoch_int, uint32_t ordinal) {
    ASSERT_ND(epoch_int < Epoch::kEpochIntOverflow);
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


  uint32_t  get_ordinal() const ALWAYS_INLINE { return data_; }
  void      set_ordinal(uint32_t ordinal) ALWAYS_INLINE {
    data_ = (data_ & (~kXctIdMaskOrdinal)) | ordinal;
  }

  void    set_being_written() ALWAYS_INLINE { data_ |= kXctIdBeingWrittenBit; }
  void    set_write_complete() ALWAYS_INLINE { data_ &= (~kXctIdBeingWrittenBit); }
  void    set_deleted() ALWAYS_INLINE { data_ |= kXctIdDeletedBit; }
  void    set_notdeleted() ALWAYS_INLINE { data_ &= (~kXctIdDeletedBit); }
  void    set_moved() ALWAYS_INLINE { data_ |= kXctIdMovedBit; }

  bool    is_being_written() const ALWAYS_INLINE { return (data_ & kXctIdBeingWrittenBit) != 0; }
  bool    is_deleted() const ALWAYS_INLINE { return (data_ & kXctIdDeletedBit) != 0; }
  bool    is_moved() const ALWAYS_INLINE { return (data_ & kXctIdMovedBit) != 0; }


  bool operator==(const XctId &other) const ALWAYS_INLINE { return data_ == other.data_; }
  bool operator!=(const XctId &other) const ALWAYS_INLINE { return data_ != other.data_; }

  /**
   * @brief Kind of std::max(this, other).
   * @details
   * This relies on the semantics of before(). Thus, this can't differentiate two XctId that
   * differ only in status bits. This method is only used for XctId generation at commit time,
   * so that's fine.
   */
  void store_max(const XctId& other) {
    if (other.get_epoch().is_valid() && before(other)) {
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
// sizeof(XctId) must be 64 bits.
STATIC_SIZE_CHECK(sizeof(XctId), sizeof(uint64_t))

struct LockableXctId {
  CombinedLock  lock_;
  XctId         xct_id_;

  McsLock* get_key_lock() ALWAYS_INLINE { return lock_.get_key_lock(); }
  bool is_keylocked() const ALWAYS_INLINE { return lock_.is_keylocked(); }
  bool is_deleted() const ALWAYS_INLINE { return xct_id_.is_deleted(); }
  bool is_moved() const ALWAYS_INLINE { return xct_id_.is_moved(); }
  bool is_being_written() const ALWAYS_INLINE { return xct_id_.is_being_written(); }

  /** used only while page initialization */
  void    reset() ALWAYS_INLINE {
    lock_.reset();
    xct_id_.data_ = 0;
  }
  friend std::ostream& operator<<(std::ostream& o, const LockableXctId& v);
};

STATIC_SIZE_CHECK(sizeof(LockableXctId), 16)

struct McsLockScope {
  McsLockScope(thread::Thread* context, LockableXctId* lock);
  McsLockScope(thread::Thread* context, McsLock* lock);
  ~McsLockScope();

  thread::Thread* context_;
  McsLock* lock_;
  McsBlockIndex block_;
};


}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
