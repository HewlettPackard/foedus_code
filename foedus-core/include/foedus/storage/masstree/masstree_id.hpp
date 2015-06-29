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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
/**
 * @file foedus/storage/masstree/masstree_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup MASSTREE
 */

#include <stdint.h>

#include <cstring>
#include <limits>

#include "foedus/compiler.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * Max number of separators stored in the first level of intermediate pages.
 * @ingroup MASSTREE
 */
const uint8_t kMaxIntermediateSeparators = 9;

/**
 * Max number of separators stored in the second level of intermediate pages.
 * @ingroup MASSTREE
 */
const uint8_t kMaxIntermediateMiniSeparators = 15;

/**
 * Max number of pointers (if completely filled) stored in an intermediate pages.
 * @ingroup MASSTREE
 */
const uint16_t kMaxIntermediatePointers
  = (kMaxIntermediateSeparators + 1U) * (kMaxIntermediateMiniSeparators + 1U);

/**
 * @brief Represents a byte-length of a key in this package.
 * @ingroup MASSTREE
 */
typedef uint16_t KeyLength;

/**
 * Max length of a key.
 * @ingroup MASSTREE
 */
const KeyLength kMaxKeyLength = 1024;

/**
 * @brief Each key slice is an 8-byte integer. Masstree pages store and compare these key slices.
 * @ingroup MASSTREE
 * @details
 * Each key-slice is in a native integer type that preserves the original order if it's made by
 * normalizing primitive key types (eg signed ints).
 * Note that this is a native integer type. This might NOT be big-endian (eg x86).
 * For signed integer types, we normalize them in an order-preserving manner, namely
 * cast to uint64_t and subtract 2^63. For unsigned integer types, no conversion needed.
 * @attention Be very careful if you give value of this type directly. In most cases, this type
 * should be only internally manipulated. The only exception is the "primitive-optimized" APIs that
 * receive this type. In that case, make sure you use normalize_primitive().
 * If the primitive is a signed type, the sign bit will bite (pun intended) you otherwise.
 */
typedef uint64_t KeySlice;

/** Shorthand for sizeof(KeySlice). Not much shorter? you are right.. */
const uint64_t kSliceLen = sizeof(KeySlice);

// infimum can be simply 0 because low-fence is inclusive.
const KeySlice kInfimumSlice = 0;
// Be aware that this might be used as a valid key slice of \e a record.
// Instead, as a fence, this is always used as a supremum.
// So, for a record key, sanity check is "slice < high_fence || (slice==high_fence==supremum)"
// For a fence, sanity check is "slice < high_fence"
const KeySlice kSupremumSlice = 0xFFFFFFFFFFFFFFFFULL;

/**
 * @brief Order-preserving normalization for primitive key types.
 * @param[in] value the value to normalize
 * @return normalized value that preserves the value-order
 * @ingroup MASSTREE
 * @tparam T primitive type of the key. All standard integers (both signed and unsigned) are
 * allowed (non-standard [u]int128_t are not supported).
 * Floats are currently not allowed because we have to normalize it to uint64_t without changing
 * value orders, which is doable but challenging (let's revisit this later).
 * @see denormalize_primitive()
 */
template<typename T>
inline KeySlice normalize_primitive(T value) {
  if (std::numeric_limits<T>::is_signed) {
    return static_cast<uint64_t>(value) - (1ULL << 63);
  } else {
    return static_cast<uint64_t>(value);
  }
}

/**
 * @brief Opposite of normalize_primitive().
 * @ingroup MASSTREE
 * @see normalize_primitive()
 */
template<typename T>
inline T denormalize_primitive(KeySlice value) {
  if (std::numeric_limits<T>::is_signed) {
    return static_cast<T>(value - (1ULL << 63));
  } else {
    return static_cast<T>(value);
  }
}

/**
 * @brief Convert an \e aligned big-endian byte array of at least 8-bytes-length to KeySlice.
 * @param[in] be_bytes a big-endian byte array. MUST BE ALIGNED.
 * @return normalized value that preserves the value-order
 * @ingroup MASSTREE
 */
inline KeySlice normalize_be_bytes_full_aligned(const void* be_bytes) {
  // then efficient.
  return assorted::read_bigendian<uint64_t>(be_bytes);
}

/**
 * @brief Convert a big-endian byte array of at least 8-bytes-length to KeySlice.
 * @param[in] be_bytes a big-endian byte array.
 * @return normalized value that preserves the value-order
 * @ingroup MASSTREE
 */
inline KeySlice normalize_be_bytes_full(const void* be_bytes) {
  // otherwise we have to copy to an aligned local variable first
  uint64_t tmp;  // 8-byte stack variable is guaranteed to be 8-byte aligned. at least in GCC.
  std::memcpy(&tmp, be_bytes, 8);
  return assorted::read_bigendian<uint64_t>(&tmp);
}

/**
 * @brief Convert a big-endian byte array of given length to KeySlice.
 * @param[in] be_bytes a big-endian byte array.
 * @param[in] length key length.
 * @return normalized value that preserves the value-order
 * @ingroup MASSTREE
 */
inline KeySlice normalize_be_bytes_fragment(const void* be_bytes, uint32_t length) {
  if (length >= 8) {
    return normalize_be_bytes_full(be_bytes);
  }
  uint64_t tmp = 0;
  std::memcpy(&tmp, be_bytes, length);
  return assorted::read_bigendian<uint64_t>(&tmp);
}

/**
 * @brief Extract a part of a big-endian byte array of given length as KeySlice.
 * @param[in] be_bytes a big-endian byte array.
 * @param[in] slice_length key length for this slice.
 * @return normalized value that preserves the value-order
 * @ingroup MASSTREE
 */
inline KeySlice slice_key(const void* be_bytes, uint16_t slice_length) {
  if (slice_length >= 8) {
    return normalize_be_bytes_full(be_bytes);
  } else {
    return normalize_be_bytes_fragment(be_bytes, slice_length);
  }
}

/**
 * @brief Extract a part of a big-endian byte array of given length as KeySlice.
 * @param[in] be_bytes a big-endian byte array.
 * @param[in] key_length total key length.
 * @param[in] current_layer extract a slice for this layer.
 * @return normalized value that preserves the value-order
 * @ingroup MASSTREE
 */
inline KeySlice slice_layer(const void* be_bytes, uint16_t key_length, uint8_t current_layer) {
  uint8_t remaining_length = key_length - current_layer * 8;
  if (remaining_length >= 8) {
    return normalize_be_bytes_full(reinterpret_cast<const char*>(be_bytes) + current_layer * 8);
  } else {
    return normalize_be_bytes_fragment(
      reinterpret_cast<const char*>(be_bytes) + current_layer * 8,
      remaining_length);
  }
}

/**
 * Returns the number of 8-byte slices that the two strings share as prefix.
 * @param[in] left aligned string pointer. can be either big-endian or little endian.
 * @param[in] right aligned string pointer must be in same endian as left.
 * @param[in] max_slices min(left_len / 8, right_len / 8)
 * @return number of shared slices
 * @ingroup MASSTREE
 */
inline uint16_t count_common_slices(const void* left, const void* right, uint16_t max_slices) {
  const uint64_t* left_casted = reinterpret_cast<const uint64_t*>(ASSUME_ALIGNED(left, 8));
  const uint64_t* right_casted = reinterpret_cast<const uint64_t*>(ASSUME_ALIGNED(right, 8));
  for (uint16_t slices = 0; slices < max_slices; ++slices) {
    if (left_casted[slices] != right_casted[slices]) {
      return slices;
    }
  }
  return max_slices;
}

/**
 * Returns if the given key is 8-bytes aligned and also zero-padded to 8-bytes
 * for easier slicing (which most of our code does). This method is usually used for assertions.
 * @ingroup MASSTREE
 */
inline bool is_key_aligned_and_zero_padded(const char* key, uint16_t key_length) {
  uintptr_t int_address = reinterpret_cast<uintptr_t>(key);
  if (int_address % 8 != 0) {
    return false;
  }
  if (key_length % 8 != 0) {
    uint16_t paddings = 8 - (key_length % 8);
    for (uint16_t i = 0; i < paddings; ++i) {
      if (key[key_length + i] != 0) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
