/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
/**
 * @file foedus/storage/masstree/masstree_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup MASSTREE
 */

#include <stdint.h>

#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * As defined in the paper, each Masstree page can have only 16 entries.
 * @ingroup MASSTREE
 */
const uint16_t kMaxSlots = 16;
/**
 * Byte size of header in each data page of masstree storage.
 * @ingroup MASSTREE
 */
const uint16_t kHeaderSize = 16;
/**
 * Byte size of data region in each data page of masstree storage.
 * @ingroup MASSTREE
 */
const uint16_t kDataSize = foedus::storage::kPageSize - kHeaderSize;

/**
 * Payload must be shorter than this length.
 * @ingroup MASSTREE
 */
const uint16_t kMaxPayload = kDataSize;

/**
 * Each key slice is an 8-byte integer. Masstree pages store and compare these key slices.
 * @ingroup MASSTREE
 */
typedef uint64_t KeySlice;

/**
 * Represents a permutation of 15 keys. See Masstree paper for more details.
 * @ingroup MASSTREE
 */
typedef uint64_t KeyPermutation;

/**
 * Most API methods have a simpler/faster version that receives this as a key,
 * which can be generated with primitive_normalize() below.
 * @ingroup MASSTREE
 */
typedef uint64_t NormalizedPrimitiveKey;

/**
 * Order-preserving normalization for primitive key types.
 * @param[in]
 * @return normalized value that preserves the value-order
 * @ingroup MASSTREE
 * @tparam T primitive type of the key. All standard integers (both signed and unsigned) are
 * allowed (non-standard [u]int128_t are not supported).
 * Floats are currently not allowed because we have to normalize it to uint64_t without changing
 * value orders, which is doable but challenging (let's revisit this later).
 */
template<typename T>
inline NormalizedPrimitiveKey primitive_normalize(T value) { return static_cast<uint64_t>(value); }

// specialize for signed types.
template<> inline NormalizedPrimitiveKey primitive_normalize<int64_t>(int64_t value) {
  return static_cast<uint64_t>(value) + (1ULL << 63);
}
template<> inline NormalizedPrimitiveKey primitive_normalize<int32_t>(int32_t value) {
  return static_cast<uint64_t>(value) + (1ULL << 63);
}
template<> inline NormalizedPrimitiveKey primitive_normalize<int16_t>(int16_t value) {
  return static_cast<uint64_t>(value) + (1ULL << 63);
}
template<> inline NormalizedPrimitiveKey primitive_normalize<int8_t>(int8_t value) {
  return static_cast<uint64_t>(value) + (1ULL << 63);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
