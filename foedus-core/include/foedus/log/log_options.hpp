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
#ifndef FOEDUS_LOG_LOG_OPTIONS_HPP_
#define FOEDUS_LOG_LOG_OPTIONS_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/fs/device_emulation_options.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/log_id.hpp"

namespace foedus {
namespace log {
/**
 * @brief Set of options for log manager.
 * @ingroup LOG
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct LogOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /** Constant values. */
  enum Constants {
    /** Default value for log_buffer_kb_. */
    kDefaultLogBufferKb = (1 << 16),
    /** Default value for log_file_size_mb_. */
    kDefaultLogSizeMb = (1 << 14),
  };
  /**
   * Constructs option values with default values.
   */
  LogOptions();

  /**
   * @brief String pattern of path of log folders in each NUMA node.
   * @details
   * This specifies the path of the folder to contain log file written out in each NUMA node.
   * Two special placeholders can be used; $NODE$ and $LOGGER$.
   * $NODE$ is replaced with the NUMA node number.
   * $LOGGER$ is replaced with the logger index in the node (0 to loggers_per_node_ - 1).
   * For example,
   * \li "/log/node_$NODE$/logger_$LOGGER$" becomes "/log/node_1/logger_0" on node-1 and logger-0.
   * \li "/log/logger_$INDEX$" becomes "/log/logger_1" on any node and logger-1.
   *
   * Both are optional. You can specify a fixed path without the patterns, which means you will
   * use the same folder for multiple loggers and nodes.
   * Even in that case, log file names include node/logger number, so it wouldn't cause any data
   * corruption. It just makes things harder for poor sysadmins.
   *
   * The default value is "logs/node_$NODE$/logger_$LOGGER$".
   */
  fs::FixedPath               folder_path_pattern_;

  /**
   * @brief Number of loggers per NUMA node.
   * @details
   * This value must be at least 1 (which is also default).
   * A larger value might be able to employ more CPU power if you have succient # of cores.
   * For the best performance, the number of loggers in each NUMA node must be
   * a submultiple of the number of cores in the node (s.t. logger assignment is balanced).
   */
  uint16_t                    loggers_per_node_;

  /** Size in KB of log buffer for \e each worker thread. */
  uint32_t                    log_buffer_kb_;

  /**
   * @brief Size in MB of each file loggers write out.
   * @details
   * The logger switches to next file when it wrote out a complete log entry and observed that
   * the current log file size is equal to or larger than this value.
   * Thus, the actual log file size might be a bit larger than this value.
   */
  uint32_t                    log_file_size_mb_;

  /**
   * @brief Whether to flush transaction logs and take savepoint when uninitialize() is called.
   * @details
   * If false, non-durable transactions since the previous savepoint is lost.
   * This allows quick shutdown when you don't care the aftermath; testcase and experiments.
   * Default is true.
   */
  bool                        flush_at_shutdown_;

  /** Settings to emulate slower logging device. */
  foedus::fs::DeviceEmulationOptions emulation_;

  /** converts folder_path_pattern_ into a string with the given IDs. */
  std::string     convert_folder_path_pattern(int node, int logger) const;
  /** construct full path of individual log file (log_folder/LOGGERID_ORDINAL.log) */
  std::string     construct_suffixed_log_path(int node, int logger, LogFileOrdinal ordinal) const;
  /** metadata log file is placed in node-0/logger-0 folder */
  std::string     construct_meta_log_path() const;

  EXTERNALIZABLE(LogOptions);
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_OPTIONS_HPP_
