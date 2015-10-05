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
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
#include <stdint.h>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_route.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace array {

/**
 * @brief Represents one data page in \ref ARRAY.
 * @ingroup ARRAY
 * @details
 * This is a private implementation-details of \ref ARRAY, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class ArrayPage final {
 public:
  /**
   * Data union for either leaf (dynamic size) or interior (fixed size).
   */
  union Data {
    char            leaf_data[kInteriorFanout * sizeof(DualPagePointer)];
    // InteriorRecord  interior_data[kInteriorFanout];
    DualPagePointer interior_data[kInteriorFanout];
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  ArrayPage() = delete;
  ArrayPage(const ArrayPage& other) = delete;
  ArrayPage& operator=(const ArrayPage& other) = delete;

  // simple accessors
  PageHeader&         header() { return header_; }
  const PageHeader&   header() const { return header_; }
  StorageId           get_storage_id()    const   { return header_.storage_id_; }
  uint16_t            get_leaf_record_count()  const {
    return kDataSize / (kRecordOverhead + assorted::align8(payload_size_));
  }
  uint16_t            get_payload_size()  const   { return payload_size_; }
  bool                is_leaf()           const   { return level_ == 0; }
  uint8_t             get_level()         const   { return level_; }
  const ArrayRange&   get_array_range()   const   { return array_range_; }

  /** Called only when this page is initialized. */
  void                initialize_snapshot_page(
    Epoch initial_epoch,
    StorageId storage_id,
    SnapshotPagePointer page_id,
    uint16_t payload_size,
    uint8_t level,
    const ArrayRange& array_range);
  void                initialize_volatile_page(
    Epoch initial_epoch,
    StorageId storage_id,
    VolatilePagePointer page_id,
    uint16_t payload_size,
    uint8_t level,
    const ArrayRange& array_range);

  // Record accesses
  const Record*  get_leaf_record(uint16_t record, uint16_t payload_size) const ALWAYS_INLINE {
    ASSERT_ND(payload_size_ == payload_size);
    ASSERT_ND(is_leaf());
    ASSERT_ND((record + 1) * (kRecordOverhead + assorted::align8(payload_size_)) <= kDataSize);
    return reinterpret_cast<const Record*>(
      data_.leaf_data + record * (kRecordOverhead + assorted::align8(payload_size_)));
  }
  Record*         get_leaf_record(uint16_t record, uint16_t payload_size) ALWAYS_INLINE {
    ASSERT_ND(payload_size_ == payload_size);
    ASSERT_ND(is_leaf());
    ASSERT_ND((record + 1) * (kRecordOverhead + assorted::align8(payload_size_)) <= kDataSize);
    return reinterpret_cast<Record*>(
      data_.leaf_data + record * (kRecordOverhead + assorted::align8(payload_size_)));
  }
  const DualPagePointer&   get_interior_record(uint16_t record) const ALWAYS_INLINE {
    return const_cast<ArrayPage*>(this)->get_interior_record(record);
  }
  DualPagePointer&         get_interior_record(uint16_t record) ALWAYS_INLINE {
    ASSERT_ND(!is_leaf());
    ASSERT_ND(record < kInteriorFanout);
    return data_.interior_data[record];
  }

  uint8_t                 unused_dummy_func_reserved1() const { return reserved1_; }
  uint32_t                unused_dummy_func_reserved2() const { return reserved2_; }

 private:
  /** common header */
  PageHeader          header_;        // +40 -> 40

  /** Byte size of one record in this array storage without internal overheads. */
  uint16_t            payload_size_;  // +2 -> 42

  /** Height of this node, counting up from 0 (leaf). */
  uint8_t             level_;         // +1 -> 43

  uint8_t             reserved1_;     // +1 -> 44
  uint32_t            reserved2_;     // +4 -> 48

  /**
   * The offset range this node is in charge of. Mainly for sanity checking.
   * If this page is right-most (eg root page), the end is the array's size,
   * which might be smaller than the range it can physically contain.
   */
  ArrayRange          array_range_;   // +16 -> 64

  // All variables up to here are immutable after the array storage is created.

  /** Dynamic records in this page. */
  Data                data_;
};

/**
 * volatile page initialize callback for ArrayPage.
 * @ingroup ARRAY
 * @see foedus::storage::VolatilePageInit
 */
void array_volatile_page_init(const VolatilePageInitArguments& args);


static_assert(sizeof(ArrayPage) == kPageSize, "sizeof(ArrayPage) is not kPageSize");
static_assert(sizeof(ArrayPage) - sizeof(ArrayPage::Data) == kHeaderSize, "kHeaderSize is wrong");

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
