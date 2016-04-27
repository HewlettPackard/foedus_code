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
#ifndef FOEDUS_STORAGE_RECORD_HPP_
#define FOEDUS_STORAGE_RECORD_HPP_
#include "foedus/cxx11.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/xct/xct_id.hpp"
namespace foedus {
namespace storage {
/**
 * @brief Represents one record in our key-value store.
 * @ingroup STORAGE
 * @details
 * This layout is used in all storage types for "value" part.
 * @attention Do NOT instantiate this object or derive from this class.
 * A record is always reinterpret-ed from a data page. No meaningful RTTI nor copy/move semantics.
 */
struct Record CXX11_FINAL {
  /**
   * This indicates the transaction that most recently modified this record.
   * This is also used as lock/delete flag.
   * Thus, for atomic operations, Record object must be 8-byte aligned.
   */
  xct::RwLockableXctId  owner_id_;

  /**
   * Arbitrary payload given by the user. The size is actually meaningless (8 is just to not
   * confuse compiler variable layout).
   */
  char                payload_[8];

  Record() CXX11_FUNC_DELETE;
  Record(const Record& other) CXX11_FUNC_DELETE;
  Record& operator=(const Record& other) CXX11_FUNC_DELETE;
};

/**
 * @brief Byte size of system-managed region per each record.
 * @ingroup ARRAY
 */
const uint16_t kRecordOverhead = sizeof(xct::RwLockableXctId);

CXX11_STATIC_ASSERT(kRecordOverhead == sizeof(Record) - 8, "kRecordOverhead is incorrect");

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_RECORD_HPP_
