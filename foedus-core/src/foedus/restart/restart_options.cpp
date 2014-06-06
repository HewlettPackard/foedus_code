/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/restart/restart_options.hpp>
namespace foedus {
namespace restart {
RestartOptions::RestartOptions() {
}

ErrorStack RestartOptions::load(tinyxml2::XMLElement* /*element*/) {
    return RET_OK;
}

ErrorStack RestartOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for restart manager"));
    return RET_OK;
}

}  // namespace restart
}  // namespace foedus
