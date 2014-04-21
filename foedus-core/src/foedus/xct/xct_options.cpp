/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/xct/xct_options.hpp>
namespace foedus {
namespace xct {
XctOptions::XctOptions() {
    max_read_set_size_ = DEFAULT_MAX_READ_SET_SIZE;
    max_write_set_size_ = DEFAULT_MAX_WRITE_SET_SIZE;
}

ErrorStack XctOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, max_read_set_size_);
    EXTERNALIZE_LOAD_ELEMENT(element, max_write_set_size_);
    return RET_OK;
}

ErrorStack XctOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for xct manager"));

    EXTERNALIZE_SAVE_ELEMENT(element, max_read_set_size_,
        "The maximum number of read-set one transaction can have. Default is 64K records.\n"
        " We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.");
    EXTERNALIZE_SAVE_ELEMENT(element, max_write_set_size_,
        "The maximum number of write-set one transaction can have. Default is 64K records.\n"
        " We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.");
    return RET_OK;
}


}  // namespace xct
}  // namespace foedus
