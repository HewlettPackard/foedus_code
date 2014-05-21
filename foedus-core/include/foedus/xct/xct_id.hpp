/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ID_HPP_
#define FOEDUS_XCT_XCT_ID_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/compiler.hpp>
#include <foedus/epoch.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/assert_nd.hpp>
#include <foedus/assorted/raw_atomics.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/assorted/atomic_fences.hpp>
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

/**
 * @brief Contents of XctId.
 * @ingroup XCT
 * @details
 * We might shrink bit size of epoch and spend it for ordinal later.
 * Be prepared for that change.
 */
union XctIdData {
    /** Entire XctId data as one integer. */
    uint64_t word;

    /**
     * Individual components of XctId data.
     * Every member must be primitive so that union constructor is trivial in both C++03 and C++11.
     */
    struct Components {
        /** The high 32 bit represents the epoch of the transaction. */
        Epoch::EpochInteger epoch_int;

        /** Middle 16 bit represents the thread (core) the transaction runs on. */
        thread::ThreadId    thread_id;

        /**
        * Uniquefier among transactions in the same epoch and thread, combined with
        * a few bits at the last. We use highests bits as status and lower bits as ordinal
        * so that we can (relatively) easily change the number of status bits later.
        */
        uint16_t            ordinal_and_status;

        Epoch               epoch() const { return Epoch(epoch_int); }
    } components;

    /**
     * To extract IDs within an epoch for logging purpose.
     * Every member must be primitive so that union constructor is trivial in both C++03 and C++11.
     */
    struct Serializers {
        /** The high 32 bit represents the epoch of the transaction. */
        Epoch::EpochInteger epoch_int;

        /** Other bits are used to serialize logs in the same epoch. */
        XctOrder            order;

        Epoch               epoch() const { return Epoch(epoch_int); }
    } serializers;
};

inline uint64_t get_xct_id_lock_bit() {
    XctIdData data;
    data.word = 0;
    data.components.ordinal_and_status = static_cast<uint16_t>(0x8000);
    return data.word;
}

const uint64_t XCT_ID_LOCK_BIT = get_xct_id_lock_bit();
const uint64_t XCT_ID_LOCK_MASK = ~XCT_ID_LOCK_BIT;

/**
 * @brief Transaction ID, a 64-bit data to identify transactions and record versions.
 * @ingroup XCT
 * @details
 * This object is equivalent to what [TU13] Sec 4.2 defines.
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctId {
    XctId() { data_.word = 0; }
    XctId(const XctId& other) ALWAYS_INLINE { data_.word = other.data_.word; }
    XctId(Epoch::EpochInteger epoch_int, thread::ThreadId thread_id, uint16_t ordinal_and_status) {
        data_.components.epoch_int = epoch_int;
        data_.components.thread_id = thread_id;
        data_.components.ordinal_and_status = ordinal_and_status;
    }
    XctId& operator=(const XctId& other) ALWAYS_INLINE {
        data_.word = other.data_.word;
        return *this;
    }

    Epoch   epoch() const ALWAYS_INLINE { return data_.components.epoch(); }
    bool    is_valid() const ALWAYS_INLINE { return epoch().is_valid(); }

    /**
     * Returns if epoch_, thread_id_, and oridnal (w/o status) are identical with the given XctId.
     * We don't provide operator== in XctId because it is confusing.
     * Instead, we provide equals_xxx that explicitly states what we are comparing.
     */
    bool equals_epoch_thread_ordinal(const XctId &other) const ALWAYS_INLINE {
        return (data_.word & XCT_ID_LOCK_MASK) == (other.data_.word & XCT_ID_LOCK_MASK);
    }
    bool equals_all(const XctId &other) const ALWAYS_INLINE {
        return data_.word == other.data_.word;
    }



    /**
     * Returns if this XctId is \e before other in serialization order, meaning this is either an
     * invalid (unused) epoch or strictly less than the other.
     * @pre other.is_valid()
     */
    bool before(const XctId &other) const ALWAYS_INLINE {
        ASSERT_ND(other.is_valid());
        return compare_epoch_thread_ordinal(other) < 0;
    }
    /** @see before() */
    bool after(const XctId &other) const ALWAYS_INLINE {
        ASSERT_ND(is_valid());
        return compare_epoch_thread_ordinal(other) > 0;
    }

    /**
     * Returns -1/0/1 if epoch_, thread_id_, and oridnal (w/o status) of this is less than/equals/
     * greather than that of other.
     */
    int compare_epoch_thread_ordinal(const XctId &other) const ALWAYS_INLINE {
        ASSERT_ND(is_valid());
        uint64_t mine = (data_.word & XCT_ID_LOCK_MASK);
        uint64_t others = (other.data_.word & XCT_ID_LOCK_MASK);
        if (mine < others) {
            return -1;
        } else if (mine == others) {
            return 0;
        } else {
            return 1;
        }
    }

    friend std::ostream& operator<<(std::ostream& o, const XctId& v);

    void lock_unconditional() {
        SPINLOCK_WHILE(true) {
            uint64_t expected = data_.word & XCT_ID_LOCK_MASK;
            uint64_t desired = expected | XCT_ID_LOCK_BIT;
            if (assorted::raw_atomic_compare_exchange_weak(&data_.word, &expected, desired)) {
                ASSERT_ND(is_locked());
                break;
            }
        }
    }
    bool is_locked() const ALWAYS_INLINE {
        return (data_.word & XCT_ID_LOCK_BIT) == XCT_ID_LOCK_BIT;
    }

    void spin_while_locked() const {
        SPINLOCK_WHILE(is_locked()) {
            assorted::memory_fence_acquire();
        }
    }

    void unlock() ALWAYS_INLINE {
        ASSERT_ND(is_locked());
        data_.word &= XCT_ID_LOCK_MASK;
    }

    /** The 64bit data. */
    XctIdData           data_;
};
// sizeof(XctId) must be 64 bits.
STATIC_SIZE_CHECK(sizeof(XctId), sizeof(uint64_t))

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
