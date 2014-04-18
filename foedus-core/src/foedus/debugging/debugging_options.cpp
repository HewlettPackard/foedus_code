/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/debugging/debugging_options.hpp>
#include <ostream>
namespace foedus {
namespace debugging {
DebuggingOptions::DebuggingOptions() :
    debug_log_to_stderr_(false),
    debug_log_stderr_threshold_(DEBUG_LOG_INFO),
    debug_log_min_threshold_(DEBUG_LOG_INFO),
    verbose_log_level_(0),
    verbose_modules_(""),
    debug_log_dir_("/tmp/") {
}
std::ostream& operator<<(std::ostream& o, const DebuggingOptions& v) {
    o << "  <DebugOptions>" << std::endl;
    EXTERNALIZE_WRITE(debug_log_to_stderr_);
    EXTERNALIZE_WRITE(debug_log_stderr_threshold_);
    EXTERNALIZE_WRITE(debug_log_min_threshold_);
    EXTERNALIZE_WRITE(verbose_log_level_);
    EXTERNALIZE_WRITE(verbose_modules_);
    EXTERNALIZE_WRITE(debug_log_dir_);
    o << "  </DebugOptions>" << std::endl;
    return o;
}

std::istream& operator<<(std::istream& in, DebuggingOptions& v) {
    return in;
}
}  // namespace debugging
}  // namespace foedus
