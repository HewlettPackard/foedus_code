/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  return kRetOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
