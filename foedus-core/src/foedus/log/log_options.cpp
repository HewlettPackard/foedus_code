/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/log/log_options.hpp>
#include <ostream>
#include <string>
namespace foedus {
namespace log {
LogOptions::LogOptions() {
    // default is
    fs::FilesystemOptions fs_options;
    fs::Filesystem filesystem(fs_options);
    fs::Path default_path = filesystem.unique_path(fs::Path("%%%%-%%%%-%%%%-%%%%.log"));
    log_paths_.push_back(default_path.string());

    thread_buffer_kb_ = DEFAULT_THREAD_BUFFER_KB;
    logger_buffer_kb_ = DEFAULT_LOGGER_BUFFER_KB;
}

std::ostream& operator<<(std::ostream& o, const LogOptions& v) {
    o << "Log options:" << std::endl;
    for (size_t i = 0; i < v.log_paths_.size(); ++i) {
        o << "  log_paths[" << i << "]=" << v.log_paths_[i] << std::endl;
    }
    o << "  thread_buffer=" << v.thread_buffer_kb_ << "KB" << std::endl;
    o << "  logger_buffer=" << v.logger_buffer_kb_ << "KB" << std::endl;
    o << v.emulation_;
    return o;
}

}  // namespace log
}  // namespace foedus
