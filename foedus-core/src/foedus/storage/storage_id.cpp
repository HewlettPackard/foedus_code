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

std::ostream& operator<<(std::ostream& o, const VolatilePagePointer& v) {
  describe_volatile_pointer(&o, v);
  return o;
}
void describe_volatile_pointer(std::ostream* o_ptr, VolatilePagePointer pointer) {
  std::ostream& o = *o_ptr;
  if (pointer.is_null()) {
    o << "<VolatilePointer is_null=\"true\"/>";
    return;
  }
  o << "<VolatilePointer"
    << " node=\"" << static_cast<int>(pointer.get_numa_node())
    << "\" offset=\"" << pointer.get_offset()
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
