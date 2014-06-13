/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/xct/xct_options.hpp>
namespace foedus {
namespace xct {
XctOptions::XctOptions() {
    max_read_set_size_ = kDefaultMaxReadSetSize;
    max_write_set_size_ = kDefaultMaxWriteSetSize;
    epoch_advance_interval_ms_ = kDefaultEpochAdvanceIntervalMs;
}

ErrorStack XctOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, max_read_set_size_);
    EXTERNALIZE_LOAD_ELEMENT(element, max_write_set_size_);
    EXTERNALIZE_LOAD_ELEMENT(element, epoch_advance_interval_ms_);
    return kRetOk;
}

ErrorStack XctOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for xct manager"));

    EXTERNALIZE_SAVE_ELEMENT(element, max_read_set_size_,
        "The maximum number of read-set one transaction can have. Default is 64K records.\n"
        " We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.");
    EXTERNALIZE_SAVE_ELEMENT(element, max_write_set_size_,
        "The maximum number of write-set one transaction can have. Default is 16K records.\n"
        " We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.");
    EXTERNALIZE_SAVE_ELEMENT(element, epoch_advance_interval_ms_,
        "Intervals in milliseconds between epoch advancements. Default is 20 ms\n"
        " Too frequent epoch advancement might become bottleneck because we synchronously write.\n"
        " out savepoint file for each non-empty epoch. However, too infrequent epoch advancement\n"
        " would increase the latency of queries because transactions are not deemed as commit"
        " until the epoch advances.");
    return kRetOk;
}


}  // namespace xct
}  // namespace foedus
