/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_storage.hpp"

#include <ostream>
#include <string>

namespace foedus {
namespace storage {
namespace array {

void ArrayStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<ArrayStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "<payload_size>" << get_payload_size() << "</payload_size>"
    << "<array_size>" << get_array_size() << "</array_size>"
    << "</ArrayStorage>";
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace array
}  // namespace storage
}  // namespace foedus
