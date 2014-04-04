/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/log_options.hpp>
#include <ostream>
namespace foedus {
namespace log {
LogOptions::LogOptions() {
    folder_paths_.push_back(".");
    thread_buffer_kb_ = DEFAULT_THREAD_BUFFER_KB;
    logger_buffer_kb_ = DEFAULT_LOGGER_BUFFER_KB;
}

}  // namespace log
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::log::LogOptions& v) {
    o << "Log options:" << std::endl;
    for (size_t i = 0; i < v.folder_paths_.size(); ++i) {
        o << "  folder_paths[" << i << "]=" << v.folder_paths_[i] << std::endl;
    }
    o << "  thread_buffer=" << v.thread_buffer_kb_ << "KB" << std::endl;
    o << "  logger_buffer=" << v.logger_buffer_kb_ << "KB" << std::endl;
    o << v.emulation_;
    return o;
}
