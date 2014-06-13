/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ID_HPP_
#define FOEDUS_XCT_XCT_ID_HPP_
#include <foedus/assert_nd.hpp>
#include <foedus/cxx11.hpp>
#include <foedus/compiler.hpp>
#include <foedus/epoch.hpp>
#include <foedus/assorted/raw_atomics.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/thread/thread_id.hpp>
#include <stdint.h>
#include <iosfwd>
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
    DIRTY_READ_PREFER_SNAPSHOT,

    /**
     * Basically same as DIRTY_READ_PREFER_SNAPSHOT, but this mode prefers volatile pages
     * if both a snapshot page and a volatile page is available. In other words,
     * more recent but more inconsistent data.
     */
    DIRTY_READ_PREFER_VOLATILE,

    /**
     * Snapshot isolation, meaning the transaction might see or be based on stale snapshot.
     * Optionally, the client can specify which snapshot we should be based on.
     */
    SNAPSHOT,

    /**
     * Protects against all anomalies in all situations.
     * This is the most expensive level, but everything good has a price.
     */
    SERIALIZABLE,
};

/**
 * Bits used to serialize (order) logs in the same epoch.
 * This is stored in many log types rather than the full XctId because epoch is implicit.
 * @ingroup XCT
 */
typedef uint32_t XctOrder;

// Defines 64bit constant values for XctId.
//                                             0123456789abcdef
const uint64_t MASK_EPOCH                  = 0xFFFFFFF000000000ULL;  // first 28 bits
const uint64_t MASK_ORDINAL                = 0x0000000FFFF00000ULL;  // next 16 bits
const uint64_t MASK_THREAD_ID              = 0x00000000000FFFF0ULL;  // next 16 bits
const uint64_t MASK_SERIALIZER             = 0xFFFFFFFFFFFFFFF0ULL;  // above 3 serialize xcts
const uint64_t MASK_IN_EPOCH_ORDER         = 0x0000000FFFFFFFF0ULL;  // ordinal and thread_id
const uint64_t KEYLOCK_BIT                 = 0x0000000000000008ULL;
const uint64_t RANGELOCK_BIT               = 0x0000000000000004ULL;
const uint64_t DELETE_BIT                  = 0x0000000000000002ULL;
const uint64_t LATEST_BIT                  = 0x0000000000000001ULL;

const uint64_t UNMASK_EPOCH                = 0x0000000FFFFFFFFFULL;
const uint64_t UNMASK_ORDINAL              = 0xFFFFFFF0000FFFFFULL;
const uint64_t UNMASK_THREAD_ID            = 0xFFFFFFFFFFF0000FULL;
const uint64_t UNMASK_KEYLOCK              = 0xFFFFFFFFFFFFFFF7ULL;
const uint64_t UNMASK_RANGELOCK            = 0xFFFFFFFFFFFFFFFBULL;
const uint64_t UNMASK_DELETE               = 0xFFFFFFFFFFFFFFFDULL;
const uint64_t UNMASK_LATEST               = 0xFFFFFFFFFFFFFFFEULL;
const uint64_t UNMASK_STATUS_BITS          = 0xFFFFFFFFFFFFFFF0ULL;

/**
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
 * <tr><td>29..45</td><td>Ordinal</td><td>The recent owning transaction had this ordinal
 * in the epoch. We assign 16 bits. Thus 64k xcts per epoch.
 * A short transaction might exceed it, but then it can just increment current epoch.
 * Also, if there are no dependencies between transactions on each core, it could be
 * up to 64k xcts per epoch per core. See commit protocol.
 * </td></tr>
 * <tr><td>46..60</td><td>ThreadId</td><td>The recent owning transaction was on this thread.
 * 16 bits. We might not need 256 node or 256 cores, but we can't be sure for future.</td></tr>
 * <tr><td>61</td><td>Key Lock bit</td><td>Lock the key.</td></tr>
 * <tr><td>62</td><td>Range Lock bit</td><td>Lock the interval from the key to next key.</td></tr>
 * <tr><td>63</td><td>Psuedo-delete bit</td><td>Logically delete the key.</td></tr>
 * <tr><td>64</td><td>(Latest bit)</td><td>So far not used. We treat snapshot differently from
 * Silo in this regard. Reserved for future/other use.</td></tr>
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
    /** Defines constant values. */
    enum Constants {
        SHIFT_EPOCH         = 36,
        SHIFT_ORDINAL       = 20,
        SHIFT_THREAD_ID     = 4,
    };

    XctId() : data_(0) {}
    XctId(const XctId& other) : data_(other.data_) {}

    void set_clean(Epoch::EpochInteger epoch_int, uint16_t ordinal, thread::ThreadId thread_id) {
        ASSERT_ND(epoch_int < Epoch::kEpochIntOverflow);
        data_ = (static_cast<uint64_t>(epoch_int) << SHIFT_EPOCH)
            | (static_cast<uint64_t>(ordinal) << SHIFT_ORDINAL)
            | (static_cast<uint64_t>(thread_id) << SHIFT_THREAD_ID);
    }

    XctId& operator=(const XctId& other) {
        data_ = other.data_;
        return *this;
    }

    Epoch   get_epoch() const ALWAYS_INLINE { return Epoch(get_epoch_int()); }
    void    set_epoch(Epoch epoch) ALWAYS_INLINE { set_epoch_int(epoch.value()); }
    Epoch::EpochInteger get_epoch_int() const ALWAYS_INLINE {
        return static_cast<Epoch::EpochInteger>((data_ & MASK_EPOCH) >> SHIFT_EPOCH);
    }
    void    set_epoch_int(Epoch::EpochInteger epoch) ALWAYS_INLINE {
        ASSERT_ND(epoch < Epoch::kEpochIntOverflow);
        data_ = (data_ & UNMASK_EPOCH) | (static_cast<uint64_t>(epoch) << SHIFT_EPOCH);
    }
    bool    is_valid() const ALWAYS_INLINE { return (data_ & MASK_EPOCH) != 0; }


    uint16_t get_ordinal() const ALWAYS_INLINE {
        return static_cast<uint16_t>((data_ & MASK_ORDINAL) >> SHIFT_ORDINAL);
    }
    void set_ordinal(uint16_t ordinal) ALWAYS_INLINE {
        data_ = (data_ & UNMASK_ORDINAL) | (static_cast<uint64_t>(ordinal) << SHIFT_ORDINAL);
    }
    thread::ThreadId get_thread_id() const ALWAYS_INLINE {
        return static_cast<thread::ThreadId>((data_ & MASK_THREAD_ID) >> SHIFT_THREAD_ID);
    }
    void    set_thread_id(thread::ThreadId id) ALWAYS_INLINE {
        data_ = (data_ & UNMASK_THREAD_ID) | (static_cast<uint64_t>(id) << SHIFT_THREAD_ID);
    }

    /**
     * Returns a 32-bit integer that represents the serial order in the epoch.
     */
    XctOrder get_in_epoch_xct_order() const ALWAYS_INLINE {
        return (data_ & MASK_IN_EPOCH_ORDER) >> SHIFT_THREAD_ID;
    }

    /**
     * Returns if epoch, thread_id, and oridnal (w/o status) are identical with the given XctId.
     */
    bool equals_serial_order(const XctId &other) const ALWAYS_INLINE {
        return (data_ & MASK_SERIALIZER) == (other.data_ & MASK_SERIALIZER);
    }
    bool equals_all(const XctId &other) const ALWAYS_INLINE {
        return data_ == other.data_;
    }

    /**
     * @brief Kind of std::max(this, other).
     * @details
     * This relies on the semantics of before(). Thus, this can't differentiate two XctId that
     * differ only in status bits. This method is only used for XctId generation at commit time,
     * so that's fine.
     */
    void store_max(const XctId& other) {
        if (before(other)) {
            data_ = other.data_;
        }
    }

    /**
     * Returns if this XctId is \e before other in serialization order, meaning this is either an
     * invalid (unused) epoch or strictly less than the other.
     * @pre other.is_valid()
     */
    bool before(const XctId &other) const ALWAYS_INLINE {
        ASSERT_ND(other.is_valid());
        if (get_epoch().before(other.get_epoch())) {
            return true;  // epoch is treated carefully because of wrap-around
        } else {
            return data_ < other.data_;  // otherwise, just an integer comparison
        }
    }

    friend std::ostream& operator<<(std::ostream& o, const XctId& v);

    /**
     * Lock this key, busy-waiting if already locked.
     * This assumes there is no deadlock (sorting write set assues it).
     */
    void keylock_unconditional() {
        SPINLOCK_WHILE(true) {
            uint64_t expected = data_ & UNMASK_KEYLOCK;
            uint64_t desired = expected | KEYLOCK_BIT;
            if (assorted::raw_atomic_compare_exchange_weak(&data_, &expected, desired)) {
                ASSERT_ND(is_keylocked());
                break;
            }
        }
    }
    bool is_keylocked() const ALWAYS_INLINE { return (data_ & KEYLOCK_BIT) != 0; }
    void spin_while_keylocked() const {
        SPINLOCK_WHILE(is_keylocked()) {
            assorted::memory_fence_acquire();
        }
    }
    void release_keylock() ALWAYS_INLINE {
        ASSERT_ND(is_keylocked());
        data_ &= UNMASK_KEYLOCK;
    }

    void rangelock_unconditional() {
        SPINLOCK_WHILE(true) {
            uint64_t expected = data_ & UNMASK_RANGELOCK;
            uint64_t desired = expected | RANGELOCK_BIT;
            if (assorted::raw_atomic_compare_exchange_weak(&data_, &expected, desired)) {
                ASSERT_ND(is_rangelocked());
                break;
            }
        }
    }
    bool is_rangelocked() const ALWAYS_INLINE { return (data_ & RANGELOCK_BIT) != 0; }
    void spin_while_rangelocked() const {
        SPINLOCK_WHILE(is_rangelocked()) {
            assorted::memory_fence_acquire();
        }
    }
    void release_rangelock() ALWAYS_INLINE {
        ASSERT_ND(is_rangelocked());
        data_ &= UNMASK_RANGELOCK;
    }

    bool is_deleted() const ALWAYS_INLINE { return (data_ & DELETE_BIT) != 0; }
    bool is_latest() const ALWAYS_INLINE { return (data_ & LATEST_BIT) != 0; }

    bool is_status_bits_off() const ALWAYS_INLINE {
        return !is_deleted() && !is_keylocked() && !is_latest() && !is_rangelocked();
    }
    void clear_status_bits() ALWAYS_INLINE {
        data_ &= UNMASK_STATUS_BITS;
    }

    /** The 64bit data. */
    uint64_t           data_;
};
// sizeof(XctId) must be 64 bits.
STATIC_SIZE_CHECK(sizeof(XctId), sizeof(uint64_t))

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
