/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_TYPES_HPP_
#define FOEDUS_LOG_LOG_TYPES_HPP_
#include <foedus/log/log_header.hpp>
/**
 * @file foedus/log/log_types.hpp
 * @brief Declares all log types used in this package.
 * @ingroup LOG
 */
namespace foedus {
namespace log {
/**
 * @brief A dummy log type to fill up a sector in log files.
 * @ingroup LOG LOGTYPE
 * @details
 * As we do direct I/O, we must do file I/O in multiply of 4kb.
 * We pad the log buffer we are about to write with this log type.
 * Log gleaner simply skips this log.
 */
struct FillerLogType {
    LOG_TYPE_NO_CONSTRUCT(FillerLogType)
    log::LogHeader  header_;
    void apply() {}
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_TYPES_HPP_
