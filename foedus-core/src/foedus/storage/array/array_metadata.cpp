/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
