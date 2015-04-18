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

#include <ostream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace storage {
namespace hash {

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
