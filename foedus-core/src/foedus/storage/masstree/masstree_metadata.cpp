/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_metadata.hpp"

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace storage {
namespace masstree {
ErrorStack MasstreeMetadata::load(tinyxml2::XMLElement* element) {
  CHECK_ERROR(load_base(element));
  EXTERNALIZE_LOAD_ELEMENT(element, border_early_split_threshold_);
  return kRetOk;
}

ErrorStack MasstreeMetadata::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(save_base(element));
  EXTERNALIZE_SAVE_ELEMENT(element, border_early_split_threshold_, "");
  return kRetOk;
}

Metadata* MasstreeMetadata::clone() const {
  MasstreeMetadata* cloned = new MasstreeMetadata();
  clone_base(cloned);
  cloned->border_early_split_threshold_ = border_early_split_threshold_;
  return cloned;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
