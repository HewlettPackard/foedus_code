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
#ifndef FOEDUS_STORAGE_HASH_HASH_ID_HPP_
#define FOEDUS_STORAGE_HASH_HASH_ID_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/storage/storage_id.hpp"

/**
 * @file foedus/storage/hash/hash_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup HASH
 */
namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief Byte size of header in an intermediate page of hash storage.
 * @ingroup HASH
 */
const uint16_t kHashIntermediatePageHeaderSize  = 64;

/**
 * @brief Number of pointers in an intermediate page of hash storage.
 * @ingroup HASH
 * @details
 * This number is quite close to a power of 2,
 * but unfortunately it's not exactly a power of two due to the page header.
 */
const uint8_t kHashIntermediatePageFanout      =
  (kPageSize - kHashIntermediatePageHeaderSize) / sizeof(DualPagePointer);

/**
 * @returns kHashIntermediatePageFanout^exponent.
 * @ingroup HASH
 */
inline uint64_t fanout_power(uint8_t exponent) {
  uint64_t ret = 1U;
  for (uint8_t i = 0; i < exponent; ++i) {
    ret *= kHashIntermediatePageFanout;
  }
  return ret;
}

/** just to write the following concisely */
const uint64_t kFanout64 = kHashIntermediatePageFanout;
/**
 * @brief kHashTotalBins[n] gives the maximum number of hash bins n-level hash can hold.
 * @ingroup HASH
 * @details
 * n is at most 7, which should be a reasonable assumption. almost 2^56.
 * Depending on the bin-bits config of the storage, it uses a smaller number of bins.
 * In other words, kHashMaxBins[level - 1] < 2^bin_bits <= kHashMaxBins[level].
 */
const uint64_t kHashMaxBins[] = {
  1ULL,
  kFanout64,
  kFanout64 * kFanout64,
  kFanout64 * kFanout64 * kFanout64,
  kFanout64 * kFanout64 * kFanout64 * kFanout64,
  kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64,
  kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64,
  kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64,
  kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64 * kFanout64,
};

/**
 * @returns levels appropriate to hold at least the given number of bins.
 * @ingroup HASH
 */
inline uint8_t bins_to_level(uint64_t bins) {
  uint8_t level;
  for (level = 1U; kHashMaxBins[level] < bins; ++level) {
    continue;
  }
  return level;
}

/**
 * @brief Max level of intermediate pages.
 * @ingroup HASH
 * @details
 * In reality 7, but let's make it 8 so that it's good for alignments in several places.
 */
const uint8_t kHashMaxLevels = 8;

/**
 * @brief Byte size of header in data page of hash storage.
 * @ingroup HASH
 */
const uint16_t kHashDataPageHeaderSize  = 128;

/**
 * @brief Represents a full 64-bit hash value calculated from a key.
 * @ingroup HASH
 * @details
 * In addition to use it as usual hash value, we extract its higher bits as \b bin.
 * Each hash storage has a static configuration that determines how many bits are used for bins.
 * Each bin represents a range of hash values, such as 0x1234560000000000 (inclusive)
 * to 0x1234570000000000 (exclusive) where bins use the high 24 bits.
 * @see HashBin
 * @see HashRange
 */
typedef uint64_t HashValue;

/**
 * @brief Represents a \b bin of a hash value.
 * @ingroup HASH
 * @details
 * As a primitive type, this is same as HashValue, but this type represents
 * an extracted hash bin. For example, if the storage uses 20 bin-bits, bin = hash >> (64-20).
 * Each bin, even empty one, consumes a 16-byte space.
 * So, 32 bin-bits consumes 64 GB even without tuple data. Most usecase would be within 32 bits.
 * However, there might be a biiiiigggggggg hash storage (NVRAM! PBs!),
 * so let's reserve 64 bits. But, this is guaranteed to be within 48 bits.
 */
typedef uint64_t HashBin;

/**
 * @brief Minimum number allowed for bin-bits.
 * @ingroup HASH
 * @details
 * 128 pointers fit in one intermediate page, so there is no point to use less than 2^7 bins.
 */
const uint8_t kHashMinBinBits = 7U;

/**
 * @brief Maximum number allowed for bin-bits.
 * @ingroup HASH
 * @details
 * Restricting to this size should be reasonable (2^48 is big!) and allows optimization
 * for 128-bit sorting.
 */
const uint8_t kHashMaxBinBits = 48U;

/** This value or larger never appears as a valid HashBin */
const HashBin kInvalidHashBin = 1ULL << kHashMaxBinBits;

/**
 * @brief Represents a range of hash bins in a hash storage, such as what an intermediate page
 * is responsible for.
 * @ingroup HASH
 * @details
 * Begin is inclusive, end is exclusive.
 * This is a range of hash bins, not full hash values.
 */
struct HashBinRange {
  HashBinRange() : begin_(0), end_(0) {}
  HashBinRange(HashBin begin, HashBin end) : begin_(begin), end_(end) {}

  bool    contains(HashBin hash) const { return hash >= begin_ && hash < end_; }
  bool    contains(const HashBinRange& other) const {
    return begin_ <= other.begin_ && end_ >= other.end_;
  }
  bool    operator==(const HashBinRange& other) const {
    return begin_ == other.begin_ && end_ == other.end_;
  }
  bool    operator!=(const HashBinRange& other) const { return !(this->operator==(other)); }

  uint64_t length() const { return end_ - begin_; }

  /** this one is NOT header-only. Defined in hash_id.cpp */
  friend std::ostream& operator<<(std::ostream& o, const HashBinRange& v);

  /** Inclusive beginning of the range. */
  HashBin begin_;
  /** Exclusive end of the range. */
  HashBin end_;
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_ID_HPP_
