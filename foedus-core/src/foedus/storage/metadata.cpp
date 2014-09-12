/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/metadata.hpp"

#include <tinyxml2.h>
#include <glog/logging.h>

#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"

namespace foedus {
namespace storage {
ErrorStack Metadata::load_base(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, id_);
  EXTERNALIZE_LOAD_ENUM_ELEMENT(element, type_);
  EXTERNALIZE_LOAD_ELEMENT(element, name_);
  EXTERNALIZE_LOAD_ELEMENT(element, root_snapshot_page_id_);
  return kRetOk;
}

ErrorStack Metadata::save_base(tinyxml2::XMLElement* element) const {
  EXTERNALIZE_SAVE_ELEMENT(element, id_, "");
  EXTERNALIZE_SAVE_ENUM_ELEMENT(element, type_, "");
  EXTERNALIZE_SAVE_ELEMENT(element, name_, "");
  EXTERNALIZE_SAVE_ELEMENT(element, root_snapshot_page_id_, "");
  return kRetOk;
}

Metadata* Metadata::create_instance(tinyxml2::XMLElement* metadata_xml) {
  int type_int = 0;
  tinyxml2::XMLElement* element = metadata_xml->FirstChildElement("type_");
  if (element) {
    tinyxml2::XMLError ret = element->QueryIntText(&type_int);
    if (ret != tinyxml2::XML_SUCCESS) {
      LOG(FATAL) << "Failed to parse storage type in metadata xml: tinyxml2 ret=" << ret;
      return nullptr;
    }
  } else {
    LOG(FATAL) << "Missing storage type in metadata xml";
    return nullptr;
  }

  StorageType type = static_cast<StorageType>(type_int);
  switch (type) {
    case kArrayStorage:
      return new array::ArrayMetadata();
    case kSequentialStorage:
      return new sequential::SequentialMetadata();
    case kHashStorage:
    case kMasstreeStorage:
      // TODO(Hideaki): Implement
    default:
      LOG(FATAL) << "Unexpected storage type:" << type;
  }
  return nullptr;
}

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
