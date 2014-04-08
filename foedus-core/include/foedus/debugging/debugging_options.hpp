/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_DEBUGGING_DEBUGGING_OPTIONS_HPP_
#define FOEDUS_DEBUGGING_DEBUGGING_OPTIONS_HPP_
#include <stdint.h>
#include <iosfwd>
#include <string>
namespace foedus {
namespace debugging {
/**
 * @brief Set of options for debugging support.
 * @ingroup DEBUGGING
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 * For ease of debugging, some of the options here has corresponding APIs to change
 * at runtime. So, those options are merely \e initial configurations.
 */
struct DebuggingOptions {
    /** Defines debug logging levels. */
    enum DebugLogLevel {
        /** Usual logs. */
        DEBUG_LOG_INFO = 0,
        /** Warns that there are something unexpected, but not a big issue. */
        DEBUG_LOG_WARNING,
        /** Raises a major issue. */
        DEBUG_LOG_ERROR,
        /** Immediately quits the engine after this log. */
        DEBUG_LOG_FATAL,
    };

    /**
     * Constructs option values with default values.
     */
    DebuggingOptions();

    /**
     * @brief Whether to write debug logs to stderr rather than log file.
     * @details
     * Default is false. There is an API to change this setting at runtime.
     */
    bool                                debug_log_to_stderr_;

    /**
     * @brief Debug logs at or above this level will be copied to stderr.
     * @details
     * Default is DEBUG_LOG_ERROR. There is an API to change this setting at runtime.
     */
    DebugLogLevel                       debug_log_stderr_threshold_;

    /**
     * @brief Debug logs below this level will be completely ignored.
     * @details
     * Default is DEBUG_LOG_INFO. There is an API to change this setting at runtime.
     */
    DebugLogLevel                       debug_log_min_threshold_;

    /**
     * @brief Verbose debug logs (VLOG(m)) at or less than this number will be shown.
     * @details
     * Default is 0. There is an API to change this setting at runtime.
     */
    uint16_t                            verbose_log_level_;

    /**
     * @brief Per-module verbose level.
     * @details
     * The value has to contain a comma-separated list of
     * 'module name'='log level'. 'module name' is a glob pattern
     * (e.g., gfs* for all modules whose name starts with "gfs"),
     * matched against the filename base (that is, name ignoring .cc/.h./-inl.h)
     * Default is "/". There is an API to change this setting at runtime.
     */
    std::string                         verbose_modules_;

    /**
     * @brief Path of the folder to write debug logs.
     * @details
     * Default is "/tmp".
     * @attention We do NOT have API to change this setting at runtime.
     * You must configure this as a start-up option.
     */
    std::string                         debug_log_dir_;

    friend std::ostream& operator<<(std::ostream& o, const DebuggingOptions& v);
};
}  // namespace debugging
}  // namespace foedus
#endif  // FOEDUS_DEBUGGING_DEBUGGING_OPTIONS_HPP_
