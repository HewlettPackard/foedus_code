/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_page_version.hpp"

#include <ostream>

namespace foedus {
namespace storage {
namespace masstree {

std::ostream& operator<<(std::ostream& o, const MasstreePageVersion& v) {
  MasstreePageVersion copied(v.data_);
  o << "<MasstreePageVersion><flags>"
    << (copied.is_locked() ? "L" : " ")
    << (copied.is_inserting() ? "I" : " ")
    << (copied.is_splitting() ? "S" : " ")
    << (copied.is_deleted() ? "D" : " ")
    << (copied.is_root() ? "R" : " ")
    << (copied.is_border() ? "B" : " ")
    << "</flags>"
    << "<insert_count>" << copied.get_insert_counter() << "</insert_count>"
    << "<split_count>" << copied.get_split_counter() << "</split_count>"
    << "<key_count>" << copied.get_key_count() << "</key_count>"
    << "</MasstreePageVersion>";
  return o;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
