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
#include "foedus/storage/array/array_metadata.hpp"

#include <iostream>
#include <sstream>
#include <string>

#include "foedus/externalize/externalizable.hpp"
namespace foedus {
namespace storage {
namespace array {
std::string ArrayMetadata::describe() const {
  std::stringstream o;
  o << ArrayMetadataSerializer(const_cast<ArrayMetadata*>(this));
  return o.str();
}
std::ostream& operator<<(std::ostream& o, const ArrayMetadata& v) {
  o << ArrayMetadataSerializer(const_cast<ArrayMetadata*>(&v));
  return o;
}

ErrorStack ArrayMetadataSerializer::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  CHECK_ERROR(get_element(element, "payload_size_", &data_casted_->payload_size_))
  CHECK_ERROR(get_element(
    element,
    "snapshot_drop_volatile_pages_threshold_",
    &data_casted_->snapshot_drop_volatile_pages_threshold_))
  CHECK_ERROR(get_element(element, "array_size_", &data_casted_->array_size_))
  return kRetOk;
}

ErrorStack ArrayMetadataSerializer::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  CHECK_ERROR(add_element(element, "payload_size_", "", data_casted_->payload_size_));
  CHECK_ERROR(add_element(
    element,
    "snapshot_drop_volatile_pages_threshold_",
    "",
    data_casted_->snapshot_drop_volatile_pages_threshold_));
  CHECK_ERROR(add_element(element, "array_size_", "", data_casted_->array_size_));
  return kRetOk;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
