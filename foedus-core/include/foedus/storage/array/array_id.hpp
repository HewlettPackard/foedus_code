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
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/storage/storage_id.hpp"

/**
 * @file foedus/storage/array/array_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup ARRAY
 */
namespace foedus {
namespace storage {
namespace array {

/**
 * @brief The only key type in array storage.
 * @ingroup ARRAY
 * @details
 * The key in array storage is \e offset, or an integer starting from zero.
 * This means we don't support multi-dimensional, dynamic, sparse, nor any other fancy arrays.
 * However, those arrays can be provided by the relational layer based on this array storage.
 * The offset-conversion is fairly straightforward.
 * @note Although it is an 8-byte integer, The valid value range of ArrayOffset is 0 to 2^48 - 1.
 * Creating an array of size 2^48 or more will fail. This won't cause any issue in reality
 * yet allows the implementation to pack more information.
 * @see kMaxArrayOffset
 */
typedef uint64_t ArrayOffset;

/**
 * @brief The maximum value allowed for ArrayOffset.
 * @ingroup ARRAY
 */
const ArrayOffset kMaxArrayOffset = (1ULL << 48) - 1ULL;

/**
 * @brief Represents an offset range in an array storage.
 * @ingroup ARRAY
 * @details
 * Begin is inclusive, end is exclusive.
 */
struct ArrayRange {
  ArrayRange() : begin_(0), end_(0) {}
  ArrayRange(ArrayOffset begin, ArrayOffset end) : begin_(begin), end_(end) {}
  /** this one adjusts the case where end might be larger than the whole array size (right-most) */
  ArrayRange(ArrayOffset begin, ArrayOffset end, ArrayOffset array_size)
    : begin_(begin), end_(end) {
    if (end > array_size) {
      end_ = array_size;
    }
  }

  /** Returns if there is any overlap with the other range. */
  bool    overlaps(const ArrayRange& other) const {
    // Case 1: contains(other.begin) or contains(other.end)
    // Case 2: not case 1, but other.contains(begin)
    return contains(other.begin_) || contains(other.end_) || other.contains(begin_);
  }
  bool    contains(ArrayOffset offset) const { return offset >= begin_ && offset < end_; }
  bool    operator==(const ArrayRange& other) const {
    return begin_ == other.begin_ && end_ == other.end_;
  }
  bool    operator!=(const ArrayRange& other) const { return !(this->operator==(other)); }

  /** Inclusive beginning of the offset range. */
  ArrayOffset begin_;
  /** Exclusive end of the offset range. */
  ArrayOffset end_;
};

/**
 * @brief Byte size of header in each page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kHeaderSize = 64;
/**
 * @brief Byte size of data region in each page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kDataSize = foedus::storage::kPageSize - kHeaderSize;
/**
 * @brief Byte size of an entry in interior page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kInteriorSize = 16;
/**
 * @brief Max number of entries in an interior page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kInteriorFanout = (foedus::storage::kPageSize - kHeaderSize) / kInteriorSize;

/**
 * @brief Code in array storage assumes this number as the maximum number of levels.
 * @ingroup ARRAY
 * @details
 * Interior page always has a big fanout close to 256, so 8 levels are more than enough.
 */
const uint8_t kMaxLevels = 8;

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
