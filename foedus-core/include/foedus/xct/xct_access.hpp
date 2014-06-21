/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ACCESS_HPP_
#define FOEDUS_XCT_XCT_ACCESS_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/storage/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief Represents a record of read-access during a transaction.
 * @ingroup XCT
 * @details
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctAccess {
  friend std::ostream& operator<<(std::ostream& o, const XctAccess& v);

  /** Transaction ID of the record observed as of the access. */
  XctId               observed_owner_id_;

  /** Pointer to the storage we accessed. */
  storage::Storage*   storage_;

  /** Pointer to the accessed record. */
  storage::Record*    record_;

  /** sort the read set in a unique order. We use address of records as done in [TU2013]. */
  static bool compare(const XctAccess &left, const XctAccess& right) {
    return reinterpret_cast<uintptr_t>(left.record_)
      < reinterpret_cast<uintptr_t>(right.record_);
  }
};


/**
 * @brief Represents a record of write-access during a transaction.
 * @ingroup XCT
 * @details
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct WriteXctAccess : public XctAccess {
  friend std::ostream& operator<<(std::ostream& o, const WriteXctAccess& v);

  /** Pointer to the log entry in private log buffer for this write opereation. */
  void*               log_entry_;

  /** sort the write set in a unique order. We use address of records as done in [TU2013]. */
  static bool compare(const WriteXctAccess &left, const WriteXctAccess& right) {
    return reinterpret_cast<uintptr_t>(left.record_)
      < reinterpret_cast<uintptr_t>(right.record_);
  }
};


}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ACCESS_HPP_
