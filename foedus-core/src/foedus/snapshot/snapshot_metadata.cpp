/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot_metadata.hpp"

#include <glog/logging.h>
#include <tinyxml2.h>

#include <memory>

#include "foedus/externalize/externalizable.hpp"
#include "foedus/storage/metadata.hpp"

namespace foedus {
namespace snapshot {

const char* kChildTagName = "storage";
const char* kStoragesTagName = "storages";
void SnapshotMetadata::clear() {
  id_ = kNullSnapshotId;
  base_epoch_ = Epoch::kEpochInvalid;
  valid_until_epoch_ = Epoch::kEpochInvalid;

  // SnapshotMetadata owns the metadata object, so we have to delete them
  for (storage::Metadata *storage : storage_metadata_) {
    delete storage;
  }
  storage_metadata_.clear();
}

ErrorStack SnapshotMetadata::load(tinyxml2::XMLElement* element) {
  clear();
  EXTERNALIZE_LOAD_ELEMENT(element, id_);
  EXTERNALIZE_LOAD_ELEMENT(element, base_epoch_);
  EXTERNALIZE_LOAD_ELEMENT(element, valid_until_epoch_);

  // <storages>
  tinyxml2::XMLElement* storages = element->FirstChildElement(kStoragesTagName);
  if (!storages) {
    // <storages> tag is missing. treat it at no storages. but weird!
    LOG(ERROR) << "WAIT, the snapshot metadata file doesn't have " << kStoragesTagName
      << " element? that's weird. It'll be treated as empty, but this shouldn't happen!";
  } else {
    for (tinyxml2::XMLElement* child = storages->FirstChildElement(kChildTagName);
      child; child = child->NextSiblingElement(kChildTagName)) {
      std::unique_ptr<storage::Metadata> metadata(storage::Metadata::create_instance(child));
      CHECK_ERROR(metadata->load(child));
      VLOG(0) << "Loaded metadata of storage-" << metadata->id_ << " from snapshot file";
      // No error, so take over the ownership from unique_ptr and put it in our vector.
      storage_metadata_.push_back(metadata.release());
    }
    LOG(INFO) << "Loaded metadata of " << storage_metadata_.size() << " storages";
  }
  // </storages>
  return kRetOk;
}

ErrorStack SnapshotMetadata::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Metadata of a snapshot"));

  EXTERNALIZE_SAVE_ELEMENT(element, id_, "Unique ID of this snapshot");
  EXTERNALIZE_SAVE_ELEMENT(element, base_epoch_,
    "This snapshot was taken based on another snapshot that is valid_until this epoch."
    " If this is the first snapshot, this value is 0.");
  EXTERNALIZE_SAVE_ELEMENT(element, valid_until_epoch_,
    "This snapshot contains all the logs until this epoch.");

  // <storages>
  tinyxml2::XMLElement* storages = element->GetDocument()->NewElement(kStoragesTagName);
  CHECK_OUTOFMEMORY(storages);
  element->InsertEndChild(storages);
  CHECK_ERROR(insert_comment(storages, "Metadata of all storages"));
  for (storage::Metadata *child : storage_metadata_) {
    CHECK_ERROR(add_child_element(storages, kChildTagName, "", *child));
  }
  // </storages>
  LOG(INFO) << "Written metadata of " << storage_metadata_.size() << " storages";
  return kRetOk;
}

}  // namespace snapshot
}  // namespace foedus
