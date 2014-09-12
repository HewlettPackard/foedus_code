/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_metadata.hpp"

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace storage {
namespace hash {
ErrorStack HashMetadata::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  EXTERNALIZE_LOAD_ELEMENT(element, bin_bits_);
  return kRetOk;
}

ErrorStack HashMetadata::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  EXTERNALIZE_SAVE_ELEMENT(element, bin_bits_, "");
  return kRetOk;
}

ErrorStack HashMetadataSerializer::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  CHECK_ERROR(get_element(element, "bin_bits_", &data_casted_->bin_bits_))
  return kRetOk;
}

ErrorStack HashMetadataSerializer::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  CHECK_ERROR(add_element(element, "bin_bits_", "", data_casted_->bin_bits_));
  return kRetOk;
}

Metadata* HashMetadata::clone() const {
  HashMetadata* cloned = new HashMetadata();
  clone_base(cloned);
  cloned->bin_bits_ = bin_bits_;
  return cloned;
}

void HashMetadata::set_capacity(uint64_t expected_records, double preferred_fillfactor) {
  if (expected_records == 0) {
    expected_records = 1;
  }
  if (preferred_fillfactor >= 1) {
    preferred_fillfactor = 1;
  }
  if (preferred_fillfactor < 0.1) {
    preferred_fillfactor = 0.1;
  }
  uint64_t bin_count = expected_records / preferred_fillfactor / kMaxEntriesPerBin;
  uint8_t bits;
  for (bits = 0; bits < 64 && ((1ULL << bits) < bin_count); ++bits) {
    continue;
  }
  if (bits < 8) {
    bits = 8;
  }
  bin_bits_ = bits;
}

void FixedHashMetadata::set_capacity(uint64_t expected_records, double preferred_fillfactor) {
  if (expected_records == 0) {
    expected_records = 1;
  }
  if (preferred_fillfactor >= 1) {
    preferred_fillfactor = 1;
  }
  if (preferred_fillfactor < 0.1) {
    preferred_fillfactor = 0.1;
  }
  uint64_t bin_count = expected_records / preferred_fillfactor / kMaxEntriesPerBin;
  uint8_t bits;
  for (bits = 0; bits < 64 && ((1ULL << bits) < bin_count); ++bits) {
    continue;
  }
  if (bits < 8) {
    bits = 8;
  }
  bin_bits_ = bits;
}


}  // namespace hash
}  // namespace storage
}  // namespace foedus
