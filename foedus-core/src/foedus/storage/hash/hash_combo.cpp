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
#include "foedus/storage/hash/hash_combo.hpp"

#include <ostream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"

namespace foedus {
namespace storage {
namespace hash {
HashCombo::HashCombo(const void* key, uint16_t key_length, const HashMetadata& meta) {
  uint8_t bin_shifts = meta.get_bin_shifts();
  hash_ = hashinate(key, key_length);
  bin_ = hash_ >> bin_shifts;
  fingerprint_ = DataPageBloomFilter::extract_fingerprint(hash_);
  route_ = IntermediateRoute::construct(bin_);
}

std::ostream& operator<<(std::ostream& o, const HashCombo& v) {
  o << "<HashCombo>"
    << "<hash>" << assorted::Hex(v.hash_, 16) << "</hash>"
    << "<bin>" << v.bin_ << "</bin>"
    << v.fingerprint_
    << v.route_
    << "</HashCombo>";
  return o;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
