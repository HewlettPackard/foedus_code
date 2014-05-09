/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ID_HPP_
#define FOEDUS_XCT_XCT_ID_HPP_
#include <foedus/cxx11.hpp>
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
 * @brief Transaction ID, a 64-bit data to identify transactions and record versions.
 * @ingroup XCT
 * @details
 * This object is equivalent to what [TU13] Sec 4.2 defines.
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 * @todo maybe the data member should be uint64_t and just a bunch of accessors to wrap it?
 */
struct XctId {
    XctId() : epoch_(), thread_id_(0), ordinal_and_status_(0) {
    }
    XctId(const XctId& other) {
        operator=(other);
    }
    XctId(Epoch epoch, thread::ThreadId thread_id, uint16_t ordinal_and_status)
        : epoch_(epoch), thread_id_(thread_id), ordinal_and_status_(ordinal_and_status) {}
    XctId& operator=(const XctId& other) {
        // to assure atomic write of 64 bits, we do the following.
        *reinterpret_cast<uint64_t*>(this) = other.as_int();
        return *this;
    }

    /**
     * Returns if epoch_, thread_id_, and oridnal (w/o status) are identical with the given XctId.
     * We don't provide operator== in XctId because it is confusing.
     * Instead, we provide compare_xxx that explicitly states what we are comparing.
     */
    bool compare_epoch_thread_ordinal(const XctId &other) const {
        return epoch_ == other.epoch_ && thread_id_ == other.thread_id_
            && (ordinal_and_status_ & 0x7FFF) == (other.ordinal_and_status_ & 0x7FFF);
    }
    bool compare_all(const XctId &other) const { return as_int() == other.as_int(); }

    friend std::ostream& operator<<(std::ostream& o, const XctId& v);

    template<unsigned int STATUS_BIT>
    void assert_status_bit() const {
        CXX11_STATIC_ASSERT(STATUS_BIT < 16, "STATUS_BIT must be within 16 bits");
        ASSERT_ND(STATUS_BIT < 16);
    }

    template<unsigned int STATUS_BIT>
    void lock_unconditional() {
        assert_status_bit<STATUS_BIT>();
        const uint16_t status_bit = static_cast<uint16_t>(1 << STATUS_BIT);
        uint64_t* target = reinterpret_cast<uint64_t*>(this);

        uint64_t expected;
        SPINLOCK_WHILE(true) {
            XctId tmp(*this);
            tmp.ordinal_and_status_ &= ~status_bit;
            expected = tmp.as_int();  // same status without lock bit
            tmp.ordinal_and_status_ |= status_bit;
            uint64_t desired = tmp.as_int();  // same status with lock bit
            if (assorted::raw_atomic_compare_exchange_weak(target, &expected, desired)) {
                ASSERT_ND(is_locked<STATUS_BIT>());
                break;
            }
        }
    }
    template<unsigned int STATUS_BIT>
    bool is_locked() const {
        assert_status_bit<STATUS_BIT>();
        const uint16_t status_bit = static_cast<uint16_t>(1 << STATUS_BIT);
        return (ordinal_and_status_ & status_bit) != 0;
    }

    template<unsigned int STATUS_BIT>
    void spin_while_locked() const {
        assert_status_bit<STATUS_BIT>();
        const uint16_t status_bit = static_cast<uint16_t>(1 << STATUS_BIT);
        SPINLOCK_WHILE((ordinal_and_status_ & status_bit) != 0) {
            assorted::memory_fence_acquire();
        }
    }

    template<unsigned int STATUS_BIT>
    void unlock() {
        assert_status_bit<STATUS_BIT>();
        const uint16_t status_bit = static_cast<uint16_t>(1 << STATUS_BIT);
        ASSERT_ND(is_locked<STATUS_BIT>());
        ordinal_and_status_ &= ~status_bit;
    }

    uint64_t    as_int() const { return *reinterpret_cast< const uint64_t* >(this); }

    /** The high 32 bit represents the epoch of the transaction. */
    Epoch               epoch_;

    /** Middle 16 bit represents the thread (core) the transaction runs on. */
    thread::ThreadId    thread_id_;

    /**
     * Uniquefier among transactions in the same epoch and thread, combined with
     * a few bits at the last. We use highests bits as status and lower bits as ordinal
     * so that we can (relatively) easily change the number of status bits later.
     */
    uint16_t            ordinal_and_status_;
};
// sizeof(XctId) must be 64 bits.
STATIC_SIZE_CHECK(sizeof(XctId), sizeof(uint64_t))

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
