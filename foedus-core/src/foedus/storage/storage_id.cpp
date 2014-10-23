/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage_id.hpp"

#include <ostream>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace storage {

void describe_snapshot_pointer(std::ostream* o_ptr, SnapshotPagePointer pointer) {
  std::ostream& o = *o_ptr;
  if (pointer == 0) {
    o << "<SnapshotPointer is_null=\"true\"/>";
    return;
  }
  o << "<SnapshotPointer"
    << " snapshot_id=\"" << extract_snapshot_id_from_snapshot_pointer(pointer)
    << "\" node=\"" << static_cast<int>(extract_numa_node_from_snapshot_pointer(pointer))
    << "\" offset=\"" << extract_local_page_id_from_snapshot_pointer(pointer)
    << "\" />";
}

void describe_volatile_pointer(std::ostream* o_ptr, VolatilePagePointer pointer) {
  std::ostream& o = *o_ptr;
  if (pointer.is_null()) {
    o << "<VolatilePointer is_null=\"true\"/>";
    return;
  }
  o << "<VolatilePointer"
    << " node=\"" << static_cast<int>(pointer.components.numa_node)
    << "\" offset=\"" << pointer.components.offset
    << "\" flags=\"" << assorted::Hex(pointer.components.flags)
    << "\" modCount=\"" << pointer.components.mod_count
    << "\" />";
}

std::ostream& operator<<(std::ostream& o, const DualPagePointer& v) {
  o << "<DualPagePointer>";
  describe_snapshot_pointer(&o, v.snapshot_pointer_);
  describe_volatile_pointer(&o, v.volatile_pointer_);
  o << "</DualPagePointer>";
  return o;
}
}  // namespace storage
}  // namespace foedus
