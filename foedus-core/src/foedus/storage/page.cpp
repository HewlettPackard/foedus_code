/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/page.hpp"

#include <glog/logging.h>

#include <ostream>

namespace foedus {
namespace storage {

std::ostream& operator<<(std::ostream& o, const PageVersion& v) {
  o << "<PageVersion><flags>"
    << (v.is_locked() ? "L" : " ")
    << (v.is_moved() ? "M" : " ")
    << (v.is_retired() ? "R" : " ")
    << "</flags><ver>" << v.get_version_counter() << "</ver>"
    << "</PageVersion>";
  return o;
}

}  // namespace storage
}  // namespace foedus
