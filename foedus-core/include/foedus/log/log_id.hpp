/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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

/**
 * a contiguous range of log entries that might span multiple files.
 * @ingroup LOG
 */
struct LogRange {
  LogFileOrdinal  begin_file_ordinal;
  LogFileOrdinal  end_file_ordinal;
  uint64_t        begin_offset;
  uint64_t        end_offset;
  bool is_empty() const {
    return begin_file_ordinal == end_file_ordinal && begin_offset == end_offset;
  }
};

}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_ID_HPP_
