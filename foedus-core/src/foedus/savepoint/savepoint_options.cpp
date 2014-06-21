/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/savepoint/savepoint_options.hpp"

#include <string>

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace savepoint {
SavepointOptions::SavepointOptions() {
  savepoint_path_ = "savepoint.xml";
}
ErrorStack SavepointOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, savepoint_path_);
  return kRetOk;
}

ErrorStack SavepointOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for savepoint manager"));
  EXTERNALIZE_SAVE_ELEMENT(element, savepoint_path_, "Full path of the savepoint file.");
  return kRetOk;
}

}  // namespace savepoint
}  // namespace foedus
