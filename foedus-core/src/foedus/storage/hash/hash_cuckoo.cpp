/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
