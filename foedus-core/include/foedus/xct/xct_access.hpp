/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_XCT_XCT_ACCESS_HPP_
#define FOEDUS_XCT_XCT_ACCESS_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/compiler.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/xct/fwd.hpp"
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
 */
struct ReadXctAccess {
  friend std::ostream& operator<<(std::ostream& o, const ReadXctAccess& v);

  /** Transaction ID of the record observed as of the access. */
  XctId               observed_owner_id_;

  /** The storage we accessed. */
  storage::StorageId  storage_id_;

  McsBlockIndex         mcs_block_;

  uint8_t*              page_hotness_address_;

  /** Pointer to the accessed record. */
  RwLockableXctId*      owner_id_address_;

  /**
   * An optional member that points to a write access related to this read.
   * For example, an insert in masstree consists of a read-access that verifies the
   * physical record and a write-access to actually install the record logically.
   * If some other thread logically installs the record between the read and write,
   * the xct must abort.
   * This member connects such "related" read-write pairs so that our commit protocol can utilize.
   */
  WriteXctAccess*     related_write_;

  /** sort the read set in a unique order. We use address of records as done in [TU2013]. */
  static bool compare(const ReadXctAccess &left, const ReadXctAccess& right) ALWAYS_INLINE;
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

  /**
   * If we have locked it, the MCS block index for the lock.
   * 0 if not locked (or locked by adjacent write-set with same owner_id address).
   */
  McsBlockIndex         mcs_block_;

  /**
   * Indicates the ordinal among WriteXctAccess of this transaction.
   * For example, first log record has 0, second 1,..
   * Next transaction of this thread resets it to 0 at begin_xct.
   * This is an auxiliary field we can potentially re-calculate from owner_id_address_,
   * but it's tedious (it's not address order when it wraps around in log buffer).
   * So, we maintain it here for sanity checks.
   * Also used for stable sorting (in case owner_id_address_ is same) of write-sets.
   */
  uint32_t              write_set_ordinal_;

  /** Pointer to the accessed record. */
  RwLockableXctId*        owner_id_address_;

  /** Pointer to the payload of the record. */
  char*                 payload_address_;

  /** Pointer to the log entry in private log buffer for this write opereation. */
  log::RecordLogType*   log_entry_;

  /** @see ReadXctAccess::related_write_ */
  ReadXctAccess*        related_read_;

  /** sort the write set in a unique order. We use address of records as done in [TU2013]. */
  static bool compare(const WriteXctAccess &left, const WriteXctAccess& right) ALWAYS_INLINE;
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

inline bool ReadXctAccess::compare(const ReadXctAccess &left, const ReadXctAccess& right) {
  return reinterpret_cast<uintptr_t>(left.owner_id_address_)
    < reinterpret_cast<uintptr_t>(right.owner_id_address_);
}

inline bool WriteXctAccess::compare(
  const WriteXctAccess &left,
  const WriteXctAccess& right) {
  uintptr_t left_address = reinterpret_cast<uintptr_t>(left.owner_id_address_);
  uintptr_t right_address = reinterpret_cast<uintptr_t>(right.owner_id_address_);
  if (left_address != right_address) {
    return left_address < right_address;
  } else {
    return left.write_set_ordinal_ < right.write_set_ordinal_;
  }
}

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_ACCESS_HPP_
