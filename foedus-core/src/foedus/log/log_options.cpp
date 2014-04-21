/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/log/log_options.hpp>
#include <string>
namespace foedus {
namespace log {
LogOptions::LogOptions() {
    log_paths_.push_back("foedus.log");
    thread_buffer_kb_ = DEFAULT_THREAD_BUFFER_KB;
    logger_buffer_kb_ = DEFAULT_LOGGER_BUFFER_KB;
}

ErrorStack LogOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, log_paths_);
    EXTERNALIZE_LOAD_ELEMENT(element, thread_buffer_kb_);
    EXTERNALIZE_LOAD_ELEMENT(element, logger_buffer_kb_);
    CHECK_ERROR(get_child_element(element, "EmulationOptions", &emulation_))
    return RET_OK;
}

ErrorStack LogOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for log manager"));

    EXTERNALIZE_SAVE_ELEMENT(element, log_paths_,
        "Full paths of log files.\n"
        " The files may or may not be on different physical devices."
        " This option also determines the number of loggers.");
    EXTERNALIZE_SAVE_ELEMENT(element, thread_buffer_kb_,
        "Size in KB of log buffer for each worker thread");
    EXTERNALIZE_SAVE_ELEMENT(element, logger_buffer_kb_, "Size in KB of logger for each logger");
    CHECK_ERROR(add_child_element(element, "XctOptions",
                    "[Experiments-only] Settings to emulate slower logging device", emulation_));
    return RET_OK;
}

}  // namespace log
}  // namespace foedus
