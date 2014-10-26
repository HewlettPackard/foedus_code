/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/metadata.hpp"

#include <tinyxml2.h>
#include <glog/logging.h>

#include <string>

#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"

namespace foedus {
namespace storage {

std::string Metadata::describe(const Metadata& metadata) {
  switch (metadata.type_) {
  case kArrayStorage:
    return static_cast<const array::ArrayMetadata&>(metadata).describe();
  case kHashStorage:
    return static_cast<const hash::HashMetadata&>(metadata).describe();
  case kMasstreeStorage:
    return static_cast<const masstree::MasstreeMetadata&>(metadata).describe();
  case kSequentialStorage:
    return static_cast<const sequential::SequentialMetadata&>(metadata).describe();
  default:
    return "Unknown metadata type";
  }
}

ErrorStack MetadataSerializer::load_base(tinyxml2::XMLElement* element) {
  CHECK_ERROR(get_element(element, "id_", &data_->id_))
  CHECK_ERROR(get_enum_element(element, "type_", &data_->type_))
  CHECK_ERROR(get_element(element, "name_", &data_->name_))
  CHECK_ERROR(get_element(element, "root_snapshot_page_id_", &data_->root_snapshot_page_id_))
  CHECK_ERROR(get_element(
    element,
    "snapshot_trigger_threshold_",
    &data_->snapshot_thresholds_.snapshot_trigger_threshold_));
  CHECK_ERROR(get_element(
    element,
    "snapshot_keep_threshold_",
    &data_->snapshot_thresholds_.snapshot_keep_threshold_));
  return kRetOk;
}

ErrorStack MetadataSerializer::save_base(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(add_element(element, "id_", "", data_->id_));
  CHECK_ERROR(add_enum_element(element, "type_", "", data_->type_));
  CHECK_ERROR(add_element(element, "name_", "", data_->name_));
  CHECK_ERROR(add_element(element, "root_snapshot_page_id_", "", data_->root_snapshot_page_id_));
  CHECK_ERROR(add_element(
    element,
    "snapshot_trigger_threshold_",
    "",
    data_->snapshot_thresholds_.snapshot_trigger_threshold_));
  CHECK_ERROR(add_element(
    element,
    "snapshot_keep_threshold_",
    "",
    data_->snapshot_thresholds_.snapshot_keep_threshold_));
  return kRetOk;
}


ErrorStack load_from_xml_array(tinyxml2::XMLElement* element, Metadata* data) {
  array::ArrayMetadataSerializer serializer(reinterpret_cast<array::ArrayMetadata*>(data));
  return serializer.load(element);
}
ErrorStack load_from_xml_hash(tinyxml2::XMLElement* element, Metadata* data) {
  hash::HashMetadataSerializer serializer(reinterpret_cast<hash::HashMetadata*>(data));
  return serializer.load(element);
}
ErrorStack load_from_xml_masstree(tinyxml2::XMLElement* element, Metadata* data) {
  masstree::MasstreeMetadataSerializer serializer(
    reinterpret_cast<masstree::MasstreeMetadata*>(data));
  return serializer.load(element);
}
ErrorStack load_from_xml_sequential(tinyxml2::XMLElement* element, Metadata* data) {
  sequential::SequentialMetadataSerializer serializer(
    reinterpret_cast<sequential::SequentialMetadata*>(data));
  return serializer.load(element);
}

const char* kChildTagName = "storage";

ErrorStack save_to_xml_array(tinyxml2::XMLElement* parent, Metadata* data) {
  array::ArrayMetadataSerializer serializer(reinterpret_cast<array::ArrayMetadata*>(data));
  return externalize::Externalizable::add_child_element(parent, kChildTagName, "", serializer);
}
ErrorStack save_to_xml_hash(tinyxml2::XMLElement* parent, Metadata* data) {
  hash::HashMetadataSerializer serializer(reinterpret_cast<hash::HashMetadata*>(data));
  return externalize::Externalizable::add_child_element(parent, kChildTagName, "", serializer);
}
ErrorStack save_to_xml_masstree(tinyxml2::XMLElement* parent, Metadata* data) {
  masstree::MasstreeMetadataSerializer serializer(
    reinterpret_cast<masstree::MasstreeMetadata*>(data));
  return externalize::Externalizable::add_child_element(parent, kChildTagName, "", serializer);
}
ErrorStack save_to_xml_sequential(tinyxml2::XMLElement* parent, Metadata* data) {
  sequential::SequentialMetadataSerializer serializer(
    reinterpret_cast<sequential::SequentialMetadata*>(data));
  return externalize::Externalizable::add_child_element(parent, kChildTagName, "", serializer);
}

ErrorStack MetadataSerializer::load_all_storages_from_xml(
  storage::StorageId largest_storage_id,
  tinyxml2::XMLElement* parent,
  StorageControlBlock* blocks) {
  uint32_t loaded_count = 0;
  for (tinyxml2::XMLElement* element = parent->FirstChildElement(kChildTagName);
    element;
    element = element->NextSiblingElement(kChildTagName)) {
    StorageId id;
    CHECK_ERROR(get_element(element, "id_", &id))
    ASSERT_ND(id > 0);
    ASSERT_ND(id <= largest_storage_id);
    StorageType type = kInvalidStorage;
    CHECK_ERROR(get_enum_element(element, "type_", &type));

    VLOG(0) << "Loading metadata of storage-" << id << " from xml";
    ASSERT_ND(blocks[id].status_ == kNotExists);
    Metadata* data = &blocks[id].meta_;
    blocks[id].status_ = kExists;
    switch (type) {
    case kArrayStorage:
      CHECK_ERROR(load_from_xml_array(element, data));
      break;
    case kHashStorage:
      CHECK_ERROR(load_from_xml_hash(element, data));
      break;
    case kMasstreeStorage:
      CHECK_ERROR(load_from_xml_masstree(element, data));
      break;
    case kSequentialStorage:
      CHECK_ERROR(load_from_xml_sequential(element, data));
      break;
    default:
      return ERROR_STACK(kErrorCodeStrUnsupportedMetadata);
    }
    ++loaded_count;
  }
  LOG(INFO) << "Loaded metadata of " << loaded_count << " storages";
  return kRetOk;
}

ErrorStack MetadataSerializer::save_all_storages_to_xml(
  storage::StorageId largest_storage_id,
  tinyxml2::XMLElement* parent,
  StorageControlBlock* blocks) {
  uint32_t saved_count = 0;
  for (storage::StorageId id = 1; id <= largest_storage_id; ++id) {
    ASSERT_ND(blocks[id].is_valid_status());
    if (!blocks[id].exists()) {
      continue;
    }
    StorageType type = blocks[id].meta_.type_;
    Metadata* data = &blocks[id].meta_;
    switch (type) {
    case kArrayStorage:
      CHECK_ERROR(save_to_xml_array(parent, data));
      break;
    case kHashStorage:
      CHECK_ERROR(save_to_xml_hash(parent, data));
      break;
    case kMasstreeStorage:
      CHECK_ERROR(save_to_xml_masstree(parent, data));
      break;
    case kSequentialStorage:
      CHECK_ERROR(save_to_xml_sequential(parent, data));
      break;
    default:
      return ERROR_STACK(kErrorCodeStrUnsupportedMetadata);
    }
    ++saved_count;
  }
  LOG(INFO) << "Written metadata of " << saved_count << " storages";
  return kRetOk;
}

}  // namespace storage
}  // namespace foedus
