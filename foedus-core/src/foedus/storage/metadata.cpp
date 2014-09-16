/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/metadata.hpp"

#include <tinyxml2.h>
#include <glog/logging.h>

namespace foedus {
namespace storage {
ErrorStack MetadataSerializer::load_base(tinyxml2::XMLElement* element) {
  CHECK_ERROR(get_element(element, "id_", &data_->id_))
  CHECK_ERROR(get_enum_element(element, "type_", &data_->type_))
  CHECK_ERROR(get_element(element, "name_", &data_->name_))
  CHECK_ERROR(get_element(element, "root_snapshot_page_id_", &data_->root_snapshot_page_id_))
  return kRetOk;
}

ErrorStack MetadataSerializer::save_base(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(add_element(element, "id_", "", data_->id_));
  CHECK_ERROR(add_enum_element(element, "type_", "", data_->type_));
  CHECK_ERROR(add_element(element, "name_", "", data_->name_));
  CHECK_ERROR(add_element(element, "root_snapshot_page_id_", "", data_->root_snapshot_page_id_));
  return kRetOk;
}

}  // namespace storage
}  // namespace foedus
