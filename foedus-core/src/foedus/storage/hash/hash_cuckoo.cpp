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
#include "foedus/storage/hash/hash_cuckoo.hpp"

#include <ostream>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace storage {
namespace hash {

std::ostream& operator<<(std::ostream& o, const HashCombo& v) {
  o << "<HashCombo>"
    << "<hash_>" << v.hash_ << "</hash_>"
    << "<bin1>" << v.bins_[0] << "</bin1>"
    << "<bin2>" << v.bins_[1] << "</bin2>"
    << "<tag>" << v.tag_ << "</tag>"
    << "<bin_bits_>" << static_cast<int>(v.bin_bits_) << "</bin_bits_>"
    << "</HashCombo>";
  return o;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
