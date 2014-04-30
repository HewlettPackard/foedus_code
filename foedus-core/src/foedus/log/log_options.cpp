/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/log/log_options.hpp>
#include <numa.h>
#include <string>
#include <sstream>
namespace foedus {
namespace log {
LogOptions::LogOptions() {
    int node_count = ::numa_num_configured_nodes();
    if (node_count <= 0) {
        node_count = 1;
    }
    for (int node = 0; node < node_count; ++node) {
        std::stringstream str;
        str << "foedus_node" << node << ".log";
        log_paths_.push_back(str.str());
    }

    log_buffer_kb_ = DEFAULT_LOG_BUFFER_KB;
    log_file_size_mb_ = DEFAULT_LOG_FILE_SIZE_MB;
}

ErrorStack LogOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, log_paths_);
    EXTERNALIZE_LOAD_ELEMENT(element, log_buffer_kb_);
    EXTERNALIZE_LOAD_ELEMENT(element, log_file_size_mb_);
    CHECK_ERROR(get_child_element(element, "LogDeviceEmulationOptions", &emulation_))
    return RET_OK;
}

ErrorStack LogOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for log manager"));

    EXTERNALIZE_SAVE_ELEMENT(element, log_paths_,
        "Full paths of log files.\n"
        " The files may or may not be on different physical devices."
        " This option also determines the number of loggers.\n"
        " For the best performance, the number of loggers must be multiply of the number of NUMA"
        " node and also be a submultiple of the total number of cores."
        " This is to evenly assign cores to loggers, loggers to NUMA nodes.");
    EXTERNALIZE_SAVE_ELEMENT(element, log_buffer_kb_, "Buffer size in KB of each worker thread");
    EXTERNALIZE_SAVE_ELEMENT(element, log_file_size_mb_, "Size in MB of files loggers write out");
    CHECK_ERROR(add_child_element(element, "LogDeviceEmulationOptions",
                    "[Experiments-only] Settings to emulate slower logging device", emulation_));
    return RET_OK;
}

}  // namespace log
}  // namespace foedus
