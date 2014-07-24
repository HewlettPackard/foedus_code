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
    << (copied.is_inserting() ? "I" : " ")
    << (copied.is_splitting() ? "S" : " ")
    << (copied.is_deleted() ? "D" : " ")
    << (copied.has_foster_child() ? "F" : " ")
    << (copied.is_high_fence_supremum() ? "H" : " ")
    << "</flags>"
    << "<insert_count>" << copied.get_insert_counter() << "</insert_count>"
    << "<split_count>" << copied.get_split_counter() << "</split_count>"
    << "<key_count>" << copied.get_key_count() << "</key_count>"
    << "</PageVersion>";
  return o;
}

void DummyVolatilePageInitializer::initialize_more(Page* /*page*/) const {
  ASSERT_ND(false);  // this should not be called
  LOG(FATAL) << "DummyVolatilePageInitializer's method was called. this must not happen";
}


}  // namespace storage
}  // namespace foedus
