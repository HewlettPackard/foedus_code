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
  o << "<SnapshotPagePointer>";
  o << "<SnapshotId>" << extract_snapshot_id_from_snapshot_pointer(pointer) << "</SnapshotId>";
  o << "<Node>" << static_cast<int>(extract_numa_node_from_snapshot_pointer(pointer)) << "</Node>";
  o << "<PageId>" << extract_local_page_id_from_snapshot_pointer(pointer) << "</PageId>";
  o << "</SnapshotPagePointer>";
}

void describe_volatile_pointer(std::ostream* o_ptr, VolatilePagePointer pointer) {
  std::ostream& o = *o_ptr;
  o << "<VolatilePagePointer>";
  o << "<Node>" << static_cast<int>(pointer.components.numa_node) << "</Node>";
  o << "<Offset>" << pointer.components.offset << "</Offset>";
  o << "<Flags>" << assorted::Hex(pointer.components.flags) << "</Flags>";
  o << "<ModCount>" << pointer.components.mod_count << "</ModCount>";
  o << "</VolatilePagePointer>";
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
