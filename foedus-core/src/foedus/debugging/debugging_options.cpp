/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/debugging/debugging_options.hpp"
#include "foedus/externalize/externalizable.hpp"
namespace foedus {
namespace debugging {
DebuggingOptions::DebuggingOptions() :
  debug_log_to_stderr_(false),
  debug_log_stderr_threshold_(kDebugLogInfo),
  debug_log_min_threshold_(kDebugLogInfo),
  verbose_log_level_(0),
  verbose_modules_(""),
  debug_log_dir_("/tmp/") {
}

ErrorStack DebuggingOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, debug_log_to_stderr_);
  EXTERNALIZE_LOAD_ENUM_ELEMENT(element, debug_log_stderr_threshold_);
  EXTERNALIZE_LOAD_ENUM_ELEMENT(element, debug_log_min_threshold_);
  EXTERNALIZE_LOAD_ELEMENT(element, verbose_log_level_);
  EXTERNALIZE_LOAD_ELEMENT(element, verbose_modules_);
  EXTERNALIZE_LOAD_ELEMENT(element, debug_log_dir_);
  return kRetOk;
}

ErrorStack DebuggingOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for debugging support.\n"
    "For ease of debugging, some of the options here has corresponding APIs to change\n"
    " at runtime. So, those options are merely initial configurations.\n"
    " enum DebugLogLevel: Defines debug logging levels\n"
    " kDebugLogInfo = 0: Usual logs\n"
    " kDebugLogWarning = 1: Warns that there are something unexpected, but not a big issue.\n"
    " kDebugLogError = 2: Raises a major issue.\n"
    " kDebugLogFatal = 3: Immediately quits the engine after this log."));

  EXTERNALIZE_SAVE_ELEMENT(element, debug_log_to_stderr_,
    "Whether to write debug logs to stderr rather than log file.\n"
    " Default is false. There is an API to change this setting at runtime.");
  EXTERNALIZE_SAVE_ENUM_ELEMENT(element, debug_log_stderr_threshold_,
    "Debug logs at or above this level will be copied to stderr.\n"
    " Default is kDebugLogInfo. There is an API to change this setting at runtime.");
  EXTERNALIZE_SAVE_ENUM_ELEMENT(element, debug_log_min_threshold_,
    "Debug logs below this level will be completely ignored.\n"
    " Default is kDebugLogInfo. There is an API to change this setting at runtime.");
  EXTERNALIZE_SAVE_ELEMENT(element, verbose_log_level_,
    "Verbose debug logs (VLOG(m)) at or less than this number will be shown.\n"
    " Default is 0. There is an API to change this setting at runtime.");
  EXTERNALIZE_SAVE_ELEMENT(element, verbose_modules_,
    "Per-module verbose level."
    " The value has to contain a comma-separated list of\n"
    " 'module name'='log level'. 'module name' is a glob pattern\n"
    " (e.g., gfs* for all modules whose name starts with 'gfs'),\n"
    " matched against the filename base (that is, name ignoring .cc/.h./-inl.h)\n"
    " Default is '/'. There is an API to change this setting at runtime.");
  EXTERNALIZE_SAVE_ELEMENT(element, debug_log_dir_,
    "Path of the folder to write debug logs.\n"
    " Default is '/tmp'. @attention We do NOT have API to change this setting at runtime.\n"
    " You must configure this as a start-up option.");
  return kRetOk;
}

}  // namespace debugging
}  // namespace foedus
