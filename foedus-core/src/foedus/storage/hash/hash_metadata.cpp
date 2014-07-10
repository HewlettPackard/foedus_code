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
  return kRetOk;
}

ErrorStack HashMetadata::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  return kRetOk;
}

Metadata* HashMetadata::clone() const {
  HashMetadata* cloned = new HashMetadata();
  clone_base(cloned);
  return cloned;
}


}  // namespace hash
}  // namespace storage
}  // namespace foedus
