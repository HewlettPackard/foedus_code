/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ID_HPP_
#define FOEDUS_XCT_XCT_ID_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/xct/epoch.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/assert_nd.hpp>
#include <stdint.h>
#include <atomic>
#include <iosfwd>
/**
 * @file foedus/xct/xct_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup XCT
 */
namespace foedus {
namespace xct {

/**
 * @brief Transaction ID, a 64-bit data to identify transactions and record versions.
 * @ingroup XCT
 * @details
 * This object is equivalent to what [TU13] Sec 4.2 defines.
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctId {
    XctId() : epoch_(), thread_id_(0), ordinal_and_status_(0) {
    }
    /**
     * Returns if epoch_ and thread_id_ are identical with the given XctId.
     * We don't provide operator== in XctId because it is confusing.
     * Instead, we provide compare_xxx that explicitly states what we are comparing.
     */
    bool compare_epoch_and_thread(const XctId &other) const {
        return epoch_ == other.epoch_ && thread_id_ == other.thread_id_;
    }
    bool compare_all(const XctId &other) const { return as_int() == other.as_int(); }

    friend std::ostream& operator<<(std::ostream& o, const XctId& v);

    template<unsigned int STATUS_BIT>
    void assert_status_bit() {
        CXX11_STATIC_ASSERT(STATUS_BIT < 16, "STATUS_BIT must be within 16 bits");
        ASSERT_ND(STATUS_BIT < 16);
    }

    template<unsigned int STATUS_BIT>
    void lock_unconditional() {
        assert_status_bit<STATUS_BIT>();
        const uint16_t status_bit = static_cast<uint16_t>(1 << STATUS_BIT);
        std::atomic<uint64_t>* address = reinterpret_cast< std::atomic<uint64_t>* >(this);

        // spin lock
        while (true) {
            XctId tmp(*this);
            tmp.ordinal_and_status_ &= ~status_bit;
            uint64_t expected = tmp.as_int();  // same status without lock bit
            tmp.ordinal_and_status_ |= status_bit;
            uint64_t desired = tmp.as_int();  // same status with lock bit
            if (address->compare_exchange_weak(expected, desired,
                std::memory_order_release, std::memory_order_acquire)) {
                break;
            }
        }
    }
    template<unsigned int STATUS_BIT>
    bool is_locked() {
        assert_status_bit<STATUS_BIT>();
        const uint16_t status_bit = static_cast<uint16_t>(1 << STATUS_BIT);
        return (ordinal_and_status_ & status_bit) != 0;
    }

    template<unsigned int STATUS_BIT>
    void unlock() {
        assert_status_bit<STATUS_BIT>();
        const uint16_t status_bit = static_cast<uint16_t>(1 << STATUS_BIT);
        ASSERT_ND((ordinal_and_status_ & status_bit) != 0);
        ordinal_and_status_ &= ~status_bit;
        std::atomic_thread_fence(std::memory_order_release);
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

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
