/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/log/log_options.hpp>
#include <ostream>
#include <string>
namespace foedus {
namespace log {
LogOptions::LogOptions() {
    log_paths_.push_back("foedus.log");
    thread_buffer_kb_ = DEFAULT_THREAD_BUFFER_KB;
    logger_buffer_kb_ = DEFAULT_LOGGER_BUFFER_KB;
}

std::ostream& operator<<(std::ostream& o, const LogOptions& v) {
    o << "  <LogOptions>" << std::endl;
    EXTERNALIZE_WRITE(log_paths_);
    EXTERNALIZE_WRITE(thread_buffer_kb_);
    EXTERNALIZE_WRITE(logger_buffer_kb_);
    o << v.emulation_;
    o << "  </LogOptions>" << std::endl;
    return o;
}

}  // namespace log
}  // namespace foedus
