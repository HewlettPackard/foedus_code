/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/metadata.hpp>
#include <foedus/storage/array/array_metadata.hpp>
#include <glog/logging.h>
#include <tinyxml2.h>
namespace foedus {
namespace storage {
ErrorStack Metadata::load_base(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, id_);
  EXTERNALIZE_LOAD_ENUM_ELEMENT(element, type_);
  EXTERNALIZE_LOAD_ELEMENT(element, name_);
  return kRetOk;
}

ErrorStack Metadata::save_base(tinyxml2::XMLElement* element) const {
  EXTERNALIZE_SAVE_ELEMENT(element, id_, "");
  EXTERNALIZE_SAVE_ENUM_ELEMENT(element, type_, "");
  EXTERNALIZE_SAVE_ELEMENT(element, name_, "");
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
    case kHashStorage:
    case kMasstreeStorage:
    case kSequentialStorage:
      // TODO(Hideaki): Implement
    default:
      LOG(FATAL) << "Unexpected storage type:" << type;
  }
  return nullptr;
}

}  // namespace storage
}  // namespace foedus
