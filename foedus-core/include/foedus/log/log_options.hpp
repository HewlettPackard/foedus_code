/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_OPTIONS_HPP_
#define FOEDUS_LOG_LOG_OPTIONS_HPP_
#include <foedus/fs/device_emulation_options.hpp>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>
namespace foedus {
namespace log {
/**
 * @brief Set of options for log manager.
 * @ingroup LOG
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct LogOptions {
    /** Constant values. */
    enum Constants {
        /** Default value for thread_buffer_kb_. */
        DEFAULT_THREAD_BUFFER_KB = 1024,
        /** Default value for logger_buffer_kb_. */
        DEFAULT_LOGGER_BUFFER_KB = 8192,
    };
    /**
     * Constructs option values with default values.
     */
    LogOptions();

    /**
     * @brief Folder paths of log folders.
     * @details
     * The folders may or may not be on different physical devices.
     * This option also determines the number of loggers.
     * @attention The default value is just one entry of current folder. When you modify this
     * setting, do NOT forget removing the default entry; call folder_paths_.clear() first.
     */
    std::vector<std::string>    folder_paths_;

    /** Size in KB of log buffer for \e each worker thread. */
    uint32_t                    thread_buffer_kb_;

    /** Size in KB of logger for \e each logger. */
    uint32_t                    logger_buffer_kb_;

    /** Settings to emulate slower logging device. */
    foedus::fs::DeviceEmulationOptions emulation_;

    friend std::ostream& operator<<(std::ostream& o, const LogOptions& v);
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_OPTIONS_HPP_
