/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
#include <stdint.h>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
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
    return kDataSize / (kRecordOverhead + payload_size_);
  }
  uint16_t            get_payload_size()  const   { return payload_size_; }
  bool                is_leaf()           const   { return level_ == 0; }
  uint8_t             get_level()         const   { return level_; }
  const ArrayRange&   get_array_range()   const   { return array_range_; }

  /** Called only when this page is initialized. */
  void                initialize_snapshot_page(
    StorageId storage_id,
    SnapshotPagePointer page_id,
    uint16_t payload_size,
    uint8_t level,
    bool    root,
    const ArrayRange& array_range);
  void                initialize_volatile_page(
    Epoch initial_epoch,
    StorageId storage_id,
    VolatilePagePointer page_id,
    uint16_t payload_size,
    uint8_t level,
    bool    root,
    const ArrayRange& array_range,
    ArrayPage* parent);

  // Record accesses
  const Record*   get_leaf_record(uint16_t record) const ALWAYS_INLINE {
    return const_cast<ArrayPage*>(this)->get_leaf_record(record);
  }
  Record*         get_leaf_record(uint16_t record) ALWAYS_INLINE {
    ASSERT_ND(is_leaf());
    ASSERT_ND((record + 1) * (kRecordOverhead + payload_size_) <= kDataSize);
    return reinterpret_cast<Record*>(data_.leaf_data
      + record * (kRecordOverhead + payload_size_));
  }
  const DualPagePointer&   get_interior_record(uint16_t record) const ALWAYS_INLINE {
    return const_cast<ArrayPage*>(this)->get_interior_record(record);
  }
  DualPagePointer&         get_interior_record(uint16_t record) ALWAYS_INLINE {
    ASSERT_ND(!is_leaf());
    ASSERT_ND(record < kInteriorFanout);
    return data_.interior_data[record];
  }

 private:
  /** common header */
  PageHeader          header_;        // +32 -> 32

  /** Byte size of one record in this array storage without internal overheads. */
  uint16_t            payload_size_;  // +2 -> 34

  /** Height of this node, counting up from 0 (leaf). */
  uint8_t             level_;         // +1 -> 35

  uint8_t             reserved1_;     // +1 -> 36
  uint32_t            reserved2_;     // +4 -> 40

  /** The offset range this node is in charge of. Mainly for sanity checking. */
  ArrayRange          array_range_;   // +16 -> 56

  uint64_t            dummy_;         // +8 -> 64

  // All variables up to here are immutable after the array storage is created.

  /** Dynamic records in this page. */
  Data                data_;
};
static_assert(sizeof(ArrayPage) == kPageSize, "sizeof(ArrayPage) is not kPageSize");
static_assert(sizeof(ArrayPage) - sizeof(ArrayPage::Data) == kHeaderSize, "kHeaderSize is wrong");

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
