/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/debugging/debugging_options.hpp>
#include <ostream>
namespace foedus {
namespace debugging {
DebuggingOptions::DebuggingOptions() :
    debug_log_to_stderr_(false),
    debug_log_stderr_threshold_(DEBUG_LOG_ERROR),
    debug_log_min_threshold_(DEBUG_LOG_INFO),
    verbose_log_level_(0),
    verbose_modules_(""),
    debug_log_dir_("/tmp/") {
}
std::ostream& operator<<(std::ostream& o, const DebuggingOptions& v) {
    o << "DebugOptions:" << std::endl;
    o << "  debug_log_to_stderr_=" << v.debug_log_to_stderr_ << std::endl;
    o << "  debug_log_stderr_threshold_=" << v.debug_log_stderr_threshold_ << std::endl;
    o << "  debug_log_min_threshold_=" << v.debug_log_min_threshold_ << std::endl;
    o << "  verbose_log_level_=" << v.verbose_log_level_ << std::endl;
    o << "  verbose_modules_=" << v.verbose_modules_ << std::endl;
    o << "  debug_log_dir_=" << v.debug_log_dir_ << std::endl;
    return o;
}
}  // namespace debugging
}  // namespace foedus
