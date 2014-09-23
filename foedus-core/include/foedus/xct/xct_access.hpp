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
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief Represents a record of following a page pointer during a transaction.
 * @ingroup XCT
 * @details
 * This is used in two cases. First, when we follow a snapshot pointer because a volatile
 * pointer was null, then we add the address of the volatile pointer to this set
 * so that we can get aware of the new version at precommit time.
 * Second, if a page pointer can be swapped for some reason (usually only a root page),
 * we add the pointer even if we are following a volatile pointer.
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct PointerAccess {
  friend std::ostream& operator<<(std::ostream& o, const PointerAccess& v);

  /** Address of the volatile pointer. */
  const storage::VolatilePagePointer* address_;

  /** Value of the volatile pointer as of the access. */
  storage::VolatilePagePointer        observed_;
};

/**
 * @brief Represents a record of reading a page during a transaction.
 * @ingroup XCT
 * @details
 * This is similar to PointerAccess. The difference is that this remembers the PageVersion
 * value we observed when we accessed the page. This can capture many more concurrency
 * issues in the page because PageVersion contains many flags and counters.
 * However, PageVersionAccess can't be used if the page itself might be swapped.
 *
 * Both PointerAccess and PageVersionAccess can be considered as "node set" in [TU2013], but
 * for a little bit different purpose.
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct PageVersionAccess {
  friend std::ostream& operator<<(std::ostream& o, const PageVersionAccess& v);

  /** Address to the page version. */
  const storage::PageVersion* address_;

  /** Value of the page version as of the access. */
  storage::PageVersionStatus observed_;
};

/**
 * @brief Represents a record of read-access during a transaction.
 * @ingroup XCT
 * @details
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 * TODO(Hideaki) should be renamed to ReadXctAccess. might be a bit lengthy tho.
 */
struct XctAccess {
  friend std::ostream& operator<<(std::ostream& o, const XctAccess& v);

  /** Transaction ID of the record observed as of the access. */
  XctId               observed_owner_id_;

  /** The storage we accessed. */
  storage::StorageId  storage_id_;

  /** Pointer to the accessed record. */
  LockableXctId*      owner_id_address_;

  /** sort the read set in a unique order. We use address of records as done in [TU2013]. */
  static bool compare(const XctAccess &left, const XctAccess& right) {
    return reinterpret_cast<uintptr_t>(left.owner_id_address_)
      < reinterpret_cast<uintptr_t>(right.owner_id_address_);
  }
};


/**
 * @brief Represents a record of write-access during a transaction.
 * @ingroup XCT
 * @details
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct WriteXctAccess {
  friend std::ostream& operator<<(std::ostream& o, const WriteXctAccess& v);

  /** The storage we accessed. */
  storage::StorageId    storage_id_;

  /** Pointer to the accessed record. */
  LockableXctId*        owner_id_address_;

  /** Pointer to the payload of the record. */
  char*                 payload_address_;

  /** Pointer to the log entry in private log buffer for this write opereation. */
  log::RecordLogType*   log_entry_;

  /**
   * If we have locked it, the MCS block index for the lock.
   * 0 if not locked (or locked by adjacent write-set with same owner_id address).
   */
  McsBlockIndex         mcs_block_;

  /** sort the write set in a unique order. We use address of records as done in [TU2013]. */
  static bool compare(const WriteXctAccess &left, const WriteXctAccess& right) {
    return reinterpret_cast<uintptr_t>(left.owner_id_address_)
      < reinterpret_cast<uintptr_t>(right.owner_id_address_);
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

  /** The storage we accessed. */
  storage::StorageId    storage_id_;

  /** Pointer to the log entry in private log buffer for this write opereation. */
  log::RecordLogType*   log_entry_;

  // no need for compare method or storing version/record/etc. it's lock-free!
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ACCESS_HPP_
