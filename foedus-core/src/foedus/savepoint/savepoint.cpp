/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/savepoint/savepoint.hpp>
namespace foedus {
namespace savepoint {
Savepoint::Savepoint() {
}

ErrorStack Savepoint::load(tinyxml2::XMLElement* element) {
    return RET_OK;
}

ErrorStack Savepoint::save(tinyxml2::XMLElement* element) const {
    return RET_OK;
}



}  // namespace savepoint
}  // namespace foedus
