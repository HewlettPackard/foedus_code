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
#include "foedus/storage/masstree/masstree_metadata.hpp"

#include <iostream>
#include <sstream>
#include <string>

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace storage {
namespace masstree {
std::string MasstreeMetadata::describe() const {
  std::stringstream o;
  o << MasstreeMetadataSerializer(const_cast<MasstreeMetadata*>(this));
  return o.str();
}
std::ostream& operator<<(std::ostream& o, const MasstreeMetadata& v) {
  o << MasstreeMetadataSerializer(const_cast<MasstreeMetadata*>(&v));
  return o;
}

ErrorStack MasstreeMetadataSerializer::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  CHECK_ERROR(get_element(
    element, "border_early_split_threshold_", &data_casted_->border_early_split_threshold_))
  CHECK_ERROR(get_element(
    element,
    "snapshot_drop_volatile_pages_layer_threshold_",
    &data_casted_->snapshot_drop_volatile_pages_layer_threshold_))
  CHECK_ERROR(get_element(
    element,
    "snapshot_drop_volatile_pages_btree_levels_",
    &data_casted_->snapshot_drop_volatile_pages_btree_levels_))
  CHECK_ERROR(get_element(element, "min_layer_hint_", &data_casted_->min_layer_hint_))
  return kRetOk;
}

ErrorStack MasstreeMetadataSerializer::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  CHECK_ERROR(add_element(
    element, "border_early_split_threshold_", "", data_casted_->border_early_split_threshold_));
  CHECK_ERROR(add_element(
    element,
    "snapshot_drop_volatile_pages_layer_threshold_",
    "",
    data_casted_->snapshot_drop_volatile_pages_layer_threshold_));
  CHECK_ERROR(add_element(
    element,
    "snapshot_drop_volatile_pages_btree_levels_",
    "",
    data_casted_->snapshot_drop_volatile_pages_btree_levels_));
  CHECK_ERROR(add_element(element, "min_layer_hint_", "", data_casted_->min_layer_hint_));
  return kRetOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
