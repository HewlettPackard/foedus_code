/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_metadata.hpp"

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace storage {
namespace sequential {
ErrorStack SequentialMetadata::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  EXTERNALIZE_LOAD_ELEMENT(element, head_root_page_id_);
  return kRetOk;
}

ErrorStack SequentialMetadata::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  EXTERNALIZE_SAVE_ELEMENT(element, head_root_page_id_, "");
  return kRetOk;
}

Metadata* SequentialMetadata::clone() const {
  SequentialMetadata* cloned = new SequentialMetadata();
  clone_base(cloned);
  cloned->head_root_page_id_ = head_root_page_id_;
  return cloned;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
