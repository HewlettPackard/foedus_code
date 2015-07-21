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
#include "foedus/log/log_options.hpp"

#include <sstream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace log {
LogOptions::LogOptions() {
  loggers_per_node_ = 1;
  folder_path_pattern_ = "logs/node_$NODE$/logger_$LOGGER$";
  log_buffer_kb_ = kDefaultLogBufferKb;
  log_file_size_mb_ = kDefaultLogSizeMb;
  flush_at_shutdown_ = true;
}

std::string LogOptions::convert_folder_path_pattern(int node, int logger) const {
  std::string tmp = assorted::replace_all(folder_path_pattern_.str(), "$NODE$", node);
  return assorted::replace_all(tmp, "$LOGGER$", logger);
}

std::string LogOptions::construct_suffixed_log_path(
  int node,
  int logger,
  LogFileOrdinal ordinal) const {
  std::string folder = convert_folder_path_pattern(node, logger);
  std::stringstream path_str;
  path_str << folder << "/" << logger << "_" << ordinal << ".log";
  return path_str.str();
}

std::string LogOptions::construct_meta_log_path() const {
  return convert_folder_path_pattern(0, 0) + std::string("/meta.log");
}

ErrorStack LogOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, folder_path_pattern_);
  EXTERNALIZE_LOAD_ELEMENT(element, loggers_per_node_);
  EXTERNALIZE_LOAD_ELEMENT(element, log_buffer_kb_);
  EXTERNALIZE_LOAD_ELEMENT(element, log_file_size_mb_);
  EXTERNALIZE_LOAD_ELEMENT(element, flush_at_shutdown_);
  CHECK_ERROR(get_child_element(element, "LogDeviceEmulationOptions", &emulation_))
  return kRetOk;
}

ErrorStack LogOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for log manager"));

  EXTERNALIZE_SAVE_ELEMENT(element, folder_path_pattern_,
    "String pattern of path of log folders in each NUMA node.\n"
    " This specifies the path of the folder to contain log file written out in each NUMA node."
    " Two special placeholders can be used; $NODE$ and $LOGGER$."
    " $NODE$ is replaced with the NUMA node number."
    " $LOGGER$ is replaced with the logger index in the node (0 to loggers_per_node_ - 1)."
    " For example,\n"
    " /log/node_$NODE$/logger_$LOGGER$ becomes /log/node_1/logger_0 on node-1 and logger-0."
    " /log/logger_$INDEX$ becomes /log/logger_1 on any node and logger-1."
    " Both are optional. You can specify a fixed path without the patterns, which means you"
    " will use the same folder for multiple loggers and nodes. Even in that case, log file"
    " names include node/logger number, so it wouldn't cause any data corruption."
    " It just makes things harder for poor sysadmins.");
  EXTERNALIZE_SAVE_ELEMENT(element, loggers_per_node_, "Number of loggers per NUMA node."
    "This value must be at least 1 (which is also default)."
    " A larger value might be able to employ more CPU power if you have succient # of cores."
    " For the best performance, the number of loggers in each NUMA node must be"
    " a submultiple of the number of cores in the node (s.t. logger assignment is balanced).");
  EXTERNALIZE_SAVE_ELEMENT(element, log_buffer_kb_, "Buffer size in KB of each worker thread");
  EXTERNALIZE_SAVE_ELEMENT(element, log_file_size_mb_, "Size in MB of files loggers write out");
  EXTERNALIZE_SAVE_ELEMENT(element, flush_at_shutdown_,
      "Whether to flush transaction logs and take savepoint when uninitialize() is called");
  CHECK_ERROR(add_child_element(element, "LogDeviceEmulationOptions",
          "[Experiments-only] Settings to emulate slower logging device", emulation_));
  return kRetOk;
}

}  // namespace log
}  // namespace foedus
