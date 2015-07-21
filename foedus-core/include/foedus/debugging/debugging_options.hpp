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
#ifndef FOEDUS_DEBUGGING_DEBUGGING_OPTIONS_HPP_
#define FOEDUS_DEBUGGING_DEBUGGING_OPTIONS_HPP_
#include <stdint.h>

#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/assorted/fixed_string.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/fs/filesystem.hpp"

namespace foedus {
namespace debugging {
/**
 * @brief Set of options for debugging support.
 * @ingroup DEBUGGING
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 * For ease of debugging, some of the options here has corresponding APIs to change
 * at runtime. So, those options are merely \e initial configurations.
 */
struct DebuggingOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /** Defines debug logging levels. */
  enum DebugLogLevel {
    /** Usual logs. */
    kDebugLogInfo = 0,
    /** Warns that there are something unexpected, but not a big issue. */
    kDebugLogWarning,
    /** Raises a major issue. */
    kDebugLogError,
    /** Immediately quits the engine after this log. */
    kDebugLogFatal,
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
   * Default is kDebugLogInfo. There is an API to change this setting at runtime.
   */
  DebugLogLevel                       debug_log_stderr_threshold_;

  /**
   * @brief Debug logs below this level will be completely ignored.
   * @details
   * Default is kDebugLogInfo. There is an API to change this setting at runtime.
   */
  DebugLogLevel                       debug_log_min_threshold_;

  /**
   * @brief Verbose debug logs (VLOG(m)) at or less than this number will be shown.
   * @details
   * Default is 0. There is an API to change this setting at runtime.
   */
  int16_t                             verbose_log_level_;

  /**
   * @brief Per-module verbose level.
   * @details
   * The value has to contain a comma-separated list of
   * 'module name'='log level'. 'module name' is a glob pattern
   * (e.g., gfs* for all modules whose name starts with "gfs"),
   * matched against the filename base (that is, name ignoring .cc/.h./-inl.h)
   * Default is "". There is an API to change this setting at runtime.
   */
  assorted::FixedString<252>          verbose_modules_;

  /**
   * @brief Path of the folder to write debug logs.
   * @details
   * Default is "/tmp".
   * @attention We do NOT have API to change this setting at runtime.
   * You must configure this as a start-up option.
   */
  fs::FixedPath                       debug_log_dir_;

  EXTERNALIZABLE(DebuggingOptions);
};
}  // namespace debugging
}  // namespace foedus
#endif  // FOEDUS_DEBUGGING_DEBUGGING_OPTIONS_HPP_
