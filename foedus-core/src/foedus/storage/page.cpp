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
  PageVersion copied(v.data_);
  o << "<PageVersion><flags>"
    << (copied.is_locked() ? "L" : " ")
    << (copied.is_moved() ? "M" : " ")
    << (copied.is_retired() ? "T" : " ")
    << "</flags>"
    << "</PageVersion>";
  return o;
}

}  // namespace storage
}  // namespace foedus
