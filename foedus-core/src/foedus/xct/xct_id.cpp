/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/xct/xct_id.hpp"

#include <ostream>

namespace foedus {
namespace xct {
std::ostream& operator<<(std::ostream& o, const XctId& v) {
  o << "<XctId><epoch>" << v.get_epoch() << "</epoch><tail_waiter>"
    << v.get_tail_waiter() << "</tail_waiter>"
    << "<ordinal>" << v.get_ordinal() << "</ordinal>"
    << "<status>"
      << (v.is_keylocked() ? "K" : " ")
      << (v.is_rangelocked() ? "R" : " ")
      << (v.is_deleted() ? "D" : " ")
      << (v.is_moved() ? "M" : " ")
    << "</status></XctId>";
  return o;
}
}  // namespace xct
}  // namespace foedus
