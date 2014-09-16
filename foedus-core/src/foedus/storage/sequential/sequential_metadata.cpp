/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_metadata.hpp"

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace storage {
namespace sequential {
ErrorStack SequentialMetadataSerializer::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  return kRetOk;
}

ErrorStack SequentialMetadataSerializer::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  return kRetOk;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
