/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ID_HPP_
#define FOEDUS_XCT_XCT_ID_HPP_
#include <foedus/xct/epoch.hpp>
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

    friend std::ostream& operator<<(std::ostream& o, const XctId& v);

    /** The high 32 bit represents the epoch of the transaction. */
    Epoch               epoch_;

    /** Middle 16 bit represents the thread (core) the transaction runs on. */
    thread::ThreadId    thread_id_;

    /**
     * Uniquefier among transactions in the same epoch and thread, combined with
     * a few bits at the last.
     */
    uint16_t            ordinal_and_status_;
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ID_HPP_
