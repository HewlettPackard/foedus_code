/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_metadata.hpp"

#include "foedus/externalize/externalizable.hpp"
namespace foedus {
namespace storage {
namespace array {
ErrorStack ArrayMetadataSerializer::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  CHECK_ERROR(get_element(element, "payload_size_", &data_casted_->payload_size_))
  CHECK_ERROR(get_element(element, "array_size_", &data_casted_->array_size_))
  return kRetOk;
}

ErrorStack ArrayMetadataSerializer::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  CHECK_ERROR(add_element(element, "payload_size_", "", data_casted_->payload_size_));
  CHECK_ERROR(add_element(element, "array_size_", "", data_casted_->array_size_));
  return kRetOk;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
