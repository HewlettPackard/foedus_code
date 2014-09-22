/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot_metadata.hpp"

#include <tinyxml2.h>
#include <glog/logging.h>

#include <memory>

#include "foedus/externalize/externalizable.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/metadata.hpp"

namespace foedus {
namespace snapshot {

const char* kStoragesTagName = "storages";
void SnapshotMetadata::clear() {
  id_ = kNullSnapshotId;
  base_epoch_ = Epoch::kEpochInvalid;
  valid_until_epoch_ = Epoch::kEpochInvalid;
  largest_storage_id_ = 0;
  storage_control_blocks_ = nullptr;
  storage_control_blocks_memory_.release_block();
}

ErrorStack SnapshotMetadata::load(tinyxml2::XMLElement* element) {
  clear();
  EXTERNALIZE_LOAD_ELEMENT(element, id_);
  EXTERNALIZE_LOAD_ELEMENT(element, base_epoch_);
  EXTERNALIZE_LOAD_ELEMENT(element, valid_until_epoch_);
  EXTERNALIZE_LOAD_ELEMENT(element, largest_storage_id_);

  uint64_t memory_size
    = static_cast<uint64_t>(largest_storage_id_) * soc::GlobalMemoryAnchors::kStorageMemorySize;
  storage_control_blocks_memory_.alloc(
    memory_size,
    1 << 12,
    memory::AlignedMemory::kNumaAllocOnnode,
    0);
  storage_control_blocks_ = reinterpret_cast<storage::StorageControlBlock*>(
    storage_control_blocks_memory_.get_block());
  std::memset(storage_control_blocks_, 0, storage_control_blocks_memory_.get_size());

  // <storages>
  tinyxml2::XMLElement* storages = element->FirstChildElement(kStoragesTagName);
  if (!storages) {
    // <storages> tag is missing. treat it as no storages. but weird!
    LOG(ERROR) << "WAIT, the snapshot metadata file doesn't have " << kStoragesTagName
      << " element? that's weird. It'll be treated as empty, but this shouldn't happen!";
  } else {
    CHECK_ERROR(storage::MetadataSerializer::load_all_storages_from_xml(
      largest_storage_id_,
      storages,
      storage_control_blocks_));
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
  EXTERNALIZE_SAVE_ELEMENT(element, largest_storage_id_,
    "The largest StorageId we so far observed.");

  // <storages>
  tinyxml2::XMLElement* storages = element->GetDocument()->NewElement(kStoragesTagName);
  CHECK_OUTOFMEMORY(storages);
  element->InsertEndChild(storages);
  CHECK_ERROR(insert_comment(storages, "Metadata of all storages"));
  CHECK_ERROR(storage::MetadataSerializer::save_all_storages_to_xml(
    largest_storage_id_,
    storages,
    storage_control_blocks_));
  // </storages>
  return kRetOk;
}
void SnapshotMetadata::assign(const externalize::Externalizable* /*other*/) {
  ASSERT_ND(false);  // should not be called
}

}  // namespace snapshot
}  // namespace foedus
