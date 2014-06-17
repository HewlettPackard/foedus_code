/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_ID_HPP_
#define FOEDUS_LOG_LOG_ID_HPP_
#include <stdint.h>
/**
 * @file foedus/log/log_id.hpp
 * @brief Typedefs of ID types used in log package.
 * @ingroup LOG
 */
namespace foedus {
namespace log {
/**
 * @typedef LoggerId
 * @brief Typedef for an ID of Logger.
 * @ingroup LOG
 * @details
 * ID of Logger is merely an ordinal without holes.
 * In other words, "(loggers_per_node * NUMA_node_id) + ordinal_in_node".
 */
typedef uint16_t LoggerId;

/**
 * @typedef LogFileOrdinal
 * @brief Ordinal of log files (eg "log.0", "log.1").
 * @ingroup LOG
 * @details
 * Each logger outputs log files whose filename is suffixed with an ordinal.
 * Each log file 
 */
typedef uint32_t LogFileOrdinal;

}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_ID_HPP_
