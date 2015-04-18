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
#ifndef FOEDUS_STORAGE_HASH_HASHINATE_HPP_
#define FOEDUS_STORAGE_HASH_HASHINATE_HPP_

#include <stdint.h>
#include <xxhash.h>

#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"

/**
 * @file foedus/storage/hash/hash_hashinate.hpp
 * @brief Independent utility methods/classes for hashination, or hash functions.
 * @ingroup HASH
 * @details
 * So far we use xxhash as a static library.
 */
namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief Default seed value used for xxhash's xxh32/64 to hashinate keys.
 * @ingroup HASH
 */
const uint64_t kXxhashKeySeed = 0x3f119e0435262a17ULL;

/**
 * @brief Calculates hash value for general input.
 * @ingroup HASH
 */
inline HashValue hashinate(const void *key, uint16_t key_length) {
  return ::XXH64(key, key_length, kXxhashKeySeed);
}

/**
 * @brief Calculates hash value for a primitive type.
 * @param[in] key Primitive key to hash
 * @tparam T Primitive type.
 * @ingroup HASH
 */
template <typename T>
inline HashValue hashinate(T key) {
  return ::XXH64(key, sizeof(T), kXxhashKeySeed);
}


/**
 * @brief Byte size of bloom filter in each HashDataPage.
 * @ingroup HASH
 * @details
 * must be a power of 2 to slightly simplify the index calculation.
 */
const uint16_t kHashDataPageBloomFilterBytes = 64;

/**
 * @brief Bit size of bloom filter in each HashDataPage.
 * @ingroup HASH
 */
const uint16_t kHashDataPageBloomFilterBits = kHashDataPageBloomFilterBytes * 8;

/**
 * @brief How many bits we need in order to represent an index in bloom filter in each HashDataPage.
 * @ingroup HASH
 * @invariant 2^kHashDataPageBloomFilterIndexSize == kHashDataPageBloomFilterBits
 */
const uint8_t kHashDataPageBloomFilterIndexSize = 9U;

CXX11_STATIC_ASSERT(
  kHashDataPageBloomFilterBits == (1U << kHashDataPageBloomFilterIndexSize),
  "If you change the size of bloom filter, change kHashDataPageBloomFilterIndexSize accordingly.");

const HashValue kHashDataPageBloomFilterIndexMask = (1U << kHashDataPageBloomFilterIndexSize) - 1U;

/**
 * @brief Number of hash functions (k) of bloom filter in each HashDataPage.
 * @ingroup HASH
 * @details
 * We consume kHashDataPageBloomFilterHashes * kHashDataPageBloomFilterIndexSize bits out of
 * the full hash value as the fingerprint.
 */
const uint8_t kHashDataPageBloomFilterHashes = 3;

/**
 * @brief A fingerprint for bloom filter in each HashDataPage.
 * @ingroup HASH
 */
struct BloomFilterFingerprint {
  /** @invariant indexes_[n] < kHashDataPageBloomFilterBits */
  uint16_t indexes_[kHashDataPageBloomFilterHashes];

  bool operator==(const BloomFilterFingerprint& other) const {
    for (uint8_t k = 0; k < kHashDataPageBloomFilterHashes; ++k) {
      if (indexes_[k] != other.indexes_[k]) {
        return false;
      }
    }
    return true;
  }
  bool operator!=(const BloomFilterFingerprint& other) const {
    return !(operator==(other));
  }
  friend std::ostream& operator<<(std::ostream& o, const BloomFilterFingerprint& v);
};

/**
 * To quickly check whether a HashDataPage \e might contain a specific hash value,
 * we maintain a non-counting bloom filter in each page.
 *
 * @par Getting Index
 * The index is determined from the hash value.
 * Higher bits are already used for bins, so all entries in this page have the same value there.
 * We avoid those bits and take lower bits as hash functions for the Bloom Filter.
 *
 * @par Size of table (m)
 * Slot is 32 bytes, so each tuple is probably 50-100 bytes or more.
 * I guess each page would contain n=30-50 records.
 *   http://corte.si/%2Fposts/code/bloom-filter-rules-of-thumb/index.html
 * Here we reserve 64 bytes (m=8*64=512) for the bloom filter, more than one-byte per item.
 *
 * @par Number of Hash Funcs (k)
 *   http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 * In our case, m/n is around 8~10. so, 3 hash functions should be practically enough.
 * 512 entries can be expressed in 9 bits. *3= 27 bits. In most cases bin-bits should be
 * within 64-27=37 bits. So, we simply take the lower 27 bits of the full hash.
 * @ingroup HASH
 */
struct DataPageBloomFilter CXX11_FINAL {
  uint8_t values_[kHashDataPageBloomFilterBytes];

  /** @return whether this page \e might contain the fingerprint */
  bool contains(const BloomFilterFingerprint& fingerprint) const {
    for (uint8_t k = 0; k < kHashDataPageBloomFilterHashes; ++k) {
      ASSERT_ND(fingerprint.indexes_[k] < kHashDataPageBloomFilterBits);
      uint8_t byte_index = fingerprint.indexes_[k] / 8U;
      uint8_t bit_index = fingerprint.indexes_[k] % 8U;
      if ((values_[byte_index] & (1U << bit_index)) == 0) {
        return false;
      }
    }
    return true;
  }

  /** @return The indexes for bloom filter extracted from a hash value. */
  static BloomFilterFingerprint extract_fingerprint(HashValue fullhash) {
    BloomFilterFingerprint ret;
    for (uint8_t k = 0; k < kHashDataPageBloomFilterHashes; ++k) {
      uint64_t index = fullhash >> (k * kHashDataPageBloomFilterIndexSize);
      // as kHashDataPageBloomFilterBits is power of 2, this is a bit simpler, but not much.
      ret.indexes_[k] = index & kHashDataPageBloomFilterIndexMask;
    }
    return ret;
  }

  friend std::ostream& operator<<(std::ostream& o, const DataPageBloomFilter& v);
};

/**
 * @brief Compactly represents the route of intermediate pages to reach the given hash bin.
 * @ingroup HASH
 * @details
 * Like array package, kHashIntermediatePageFanout is less than 256, so we have a similar route
 * struct. The only difference is that this is about intermediate pages (bins).
 * Data pages have no notion of route unlike array package.
 */
union IntermediateRoute {
  /** This is a 64bit data. */
  uint64_t word;
  /**
   * [0] means ordinal in leaf intermediate page, [1] in its parent page, [2]...
   * [levels - 1] is the ordinal in root intermediate page.
   */
  uint8_t route[8];

  bool operator==(const IntermediateRoute& rhs) const { return word == rhs.word; }
  bool operator!=(const IntermediateRoute& rhs) const { return word != rhs.word; }
  bool operator<(const IntermediateRoute& rhs) const {
    for (uint16_t i = 1; i <= 8U; ++i) {
      // compare the higher level first. if the machine is big-endian, we can just compare word.
      // but, this method is not used in performance-sensitive place, so let's be explicit.
      if (route[8U - i] != rhs.route[8U - i]) {
        return route[8U - i] < rhs.route[8U - i];
      }
    }
    return false;
  }
  bool operator<=(const IntermediateRoute& rhs) const { return *this == rhs || *this < rhs; }
  friend std::ostream& operator<<(std::ostream& o, const IntermediateRoute& v);

  static IntermediateRoute construct(HashBin bin) {
    IntermediateRoute ret;
    ret.word = 0;
    uint8_t level = 0;
    while (bin > 0) {
      // in hash, fanout is always fixed, so no need for pre-calculated const_div. compiler does it.
      ret.route[level] = bin % kHashIntermediatePageFanout;
      bin /= kHashIntermediatePageFanout;
      ++level;
      ASSERT_ND(level < 8U);
    }

    return ret;
  }
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASHINATE_HPP_
