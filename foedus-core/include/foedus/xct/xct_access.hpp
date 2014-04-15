/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ACCESS_HPP_
#define FOEDUS_XCT_XCT_ACCESS_HPP_
#include <foedus/storage/fwd.hpp>
#include <foedus/xct/xct_id.hpp>
#include <iosfwd>
namespace foedus {
namespace xct {

/**
 * @brief Represents a record of read-access or write-access during a transaction.
 * @ingroup XCT
 * @details
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctAccess {
    friend std::ostream& operator<<(std::ostream& o, const XctAccess& v);

    /** Transaction ID of the record observed as of the access. */
    XctId               observed_owner_id_;

    /** Pointer to the accessed record. */
    storage::Record*    record_;
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ACCESS_HPP_
