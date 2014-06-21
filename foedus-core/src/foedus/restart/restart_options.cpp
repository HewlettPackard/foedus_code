/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/externalize/externalizable.hpp"
#include "foedus/restart/restart_options.hpp"
namespace foedus {
namespace restart {
RestartOptions::RestartOptions() {
}

ErrorStack RestartOptions::load(tinyxml2::XMLElement* /*element*/) {
  return kRetOk;
}

ErrorStack RestartOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for restart manager"));
  return kRetOk;
}

}  // namespace restart
}  // namespace foedus
