/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_ACCESS_HPP_
#define FOEDUS_XCT_XCT_ACCESS_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/log/fwd.hpp"
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
  log::RecordLogType*   log_entry_;

  /** sort the write set in a unique order. We use address of records as done in [TU2013]. */
  static bool compare(const WriteXctAccess &left, const WriteXctAccess& right) {
    return reinterpret_cast<uintptr_t>(left.record_)
      < reinterpret_cast<uintptr_t>(right.record_);
  }
};


/**
 * @brief Represents a record of special write-access during a transaction
 * without any need for locking.
 * @ingroup XCT
 * @details
 * Some storage type doesn't need locking for serializability (so far \ref SEQUENTIAL only)
 * For them, we maintain this write-set objects separated from WriteXctAccess.
 * We don't lock/unlock for these records, and we don't even have to remember what
 * we observed (actually, we don't even observe anything when we create this).
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct LockFreeWriteXctAccess {
  friend std::ostream& operator<<(std::ostream& o, const LockFreeWriteXctAccess& v);

  /** Pointer to the storage we accessed. */
  storage::Storage*     storage_;

  /** Pointer to the log entry in private log buffer for this write opereation. */
  log::RecordLogType*   log_entry_;

  // no need for compare method or storing version/record/etc. it's lock-free!
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ACCESS_HPP_
