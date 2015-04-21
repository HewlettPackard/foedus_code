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
#include "foedus/storage/hash/hash_hashinate.hpp"

#include <xxhash.h>

#include <ostream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace storage {
namespace hash {

HashValue hashinate(const void *key, uint16_t key_length) {
  return ::XXH64(key, key_length, kXxhashKeySeed);
}

/**
 * @brief Calculates hash value for a primitive type.
 * @param[in] key Primitive key to hash
 * @tparam T Primitive type.
 * @ingroup HASH
 */
template <typename T>
HashValue hashinate(T key) {
  return ::XXH64(&key, sizeof(T), kXxhashKeySeed);
}

// @cond DOXYGEN_IGNORE
// explicit instantiation of primitive version of hashinate()
#define EXP_HASHINATE(x) template HashValue hashinate< x >(x key)
INSTANTIATE_ALL_NUMERIC_TYPES(EXP_HASHINATE);
// also 128bit types. we might use it...
template HashValue hashinate< __uint128_t >(__uint128_t key);
template HashValue hashinate< __int128_t >(__int128_t key);
// @endcond

std::ostream& operator<<(std::ostream& o, const BloomFilterFingerprint& v) {
  o << "<Fingerprint indexes=\"";
  for (uint8_t i = 0; i < kHashDataPageBloomFilterHashes; ++i) {
    o << v.indexes_[i] << ",";
  }
  o << "\" />";
  return o;
}

std::ostream& operator<<(std::ostream& o, const IntermediateRoute& v) {
  o << "<Route>";
  for (uint8_t i = 0; i < 8U; ++i) {
    o << assorted::Hex(v.route[i], 2) << " ";
  }
  o << "</Route>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const DataPageBloomFilter& v) {
  o << "<DataPageBloomFilter>" << std::endl;
  // one line for 8 bytes
  for (uint16_t i = 0; i * 8U < kHashDataPageBloomFilterBytes; ++i) {
    o << "  <Bytes row=\"" << i << "\">";
    for (uint16_t j = i * 8U; j < (i + 1U) * 8U && j < kHashDataPageBloomFilterBytes; ++j) {
      o << assorted::Hex(v.values_[j], 2) << " ";
    }
    o << "</Bytes>" << std::endl;
  }
  o << "</DataPageBloomFilter>";
  return o;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
