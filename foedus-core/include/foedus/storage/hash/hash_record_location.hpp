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
#ifndef FOEDUS_STORAGE_HASH_RECORD_LOCATION_HPP_
#define FOEDUS_STORAGE_HASH_RECORD_LOCATION_HPP_

#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace hash {

/**
  * @brief return value of locate_record().
  * @details
  * @par Notes on XID, MOCC Locking, read-set, and "physical_only"
  * This object also contains \e observed_, which is guaranteed to be the XID at or before
  * the return from locate_record(). There is a notable contract here.
  * When locking in MOCC kicks in, we want to make sure we return the value of XID \e after
  * the lock to eliminate a chance of false verification failure.
  * However, this means we need to take lock and register read-set \e within locate_record().
  * This caused us headaches for the following reason.
  *
  * @par Logical vs Physical search in hash pages
  * \e Physically finding a record for given key in our hash page has the following guarantees.
  * \li When it finds a record, the record was at least at some point a valid
  * (non-moved/non-being-written) record of the given key.
  * \li When it doesn't find any record, the page(s) at least at some point didn't contain
  * any valid record of the given key.
  *
  * On the other hand, it still allows the following behaviors:
  * \li The found record is now being moved or modified.
  * \li The page or its next page(s) is now newly inserting a physical record of the given key.
  *
  * \e Logically finding a record provides additional guarantee to protect against the above
  * at pre-commit time. It additionally takes read-set, page-version set, or even MOCC locks.
  *
  * @par locate_record(): Logical or Physical
  * We initially designed locate_record() as a physical-only search operation
  * separated from logical operations (e.g., get_record/overwrite_record, or the caller).
  * Separation makes each of them simpler and more flexible.
  * The logical side can separately decide, using an operation-specific logic, whether/when it
  * will add the XID observed in locate_record() to read-set or not.
  * The physical side (locate_record()) can be dumb simple, too.
  *
  * But, seems like this is now unavoidable.
  * Hence now locate_record() is a logical operation.
  * To allow the caller to choose what to do logically, we receive
  * a paramter \e physical_only. When false, locate_record \e might take logical lock and add
  * the XID to readset, hence what locate_record() observed is protected.
  * When true, locate_record never takes lock or adds it to readset.
  * It just physically locates a record that was at least at some point a valid record.
  * In this case,
  * the caller is responsible to do appropriate thing to protect from concurrent modifications.
  */
struct RecordLocation {
  /** The data page (might not be bin-head) containing the record. */
  HashDataPage* page_;
  /** Index of the record in the page. kSlotNotFound if not found. */
  DataPageSlotIndex index_;
  /**
   * Logical payload length \b as-of the observed XID.
   * payload_length_ in hash page slot is \b mutable, so this must be paired
   * with observed_ in serializable transactions.
   */
  uint16_t cur_payload_length_;
  /** Key length of the slot, which is immutable. */
  uint16_t key_length_;
  /** Byte count the record of the slot occupies, which is immutable. */
  uint16_t physical_record_length_;

  /** Address of the record. null if not found. */
  char* record_;
  /**
    * TID as of locate_record() identifying the record.
    * guaranteed to be not is_moved (then automatically retried), though the \b current
    * TID might be now moved, in which case pre-commit will catch it.
    * See the class comment.
    */
  xct::XctId observed_;

  /**
    * If this method took a read-set on the returned record,
    * points to the corresponding read-set. Otherwise nullptr.
    */
  xct::ReadXctAccess* readset_;

  bool is_found() const { return index_ != kSlotNotFound; }
  inline uint16_t get_aligned_key_length() const { return assorted::align8(key_length_); }
  inline uint16_t get_max_payload() const {
    return physical_record_length_ - get_aligned_key_length();
  }

  void clear() {
    page_ = CXX11_NULLPTR;
    index_ = kSlotNotFound;
    cur_payload_length_ = 0;
    key_length_ = 0;
    physical_record_length_ = 0;
    record_ = CXX11_NULLPTR;
    observed_.data_ = 0;
    readset_ = CXX11_NULLPTR;
  }

  /**
    * Populates the result with XID and possibly readset.
    * This is a \e logical operation that might take a readset.
    */
  ErrorCode populate_logical(
    xct::Xct* cur_xct,
    HashDataPage* page,
    DataPageSlotIndex index,
    bool intended_for_write);

  /**
   * Populates fields other than readset_.
   * This is a \e physical-only operation that never takes readset/locks.
   */
  void populate_physical(HashDataPage* page, DataPageSlotIndex index);
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_RECORD_LOCATION_HPP_
