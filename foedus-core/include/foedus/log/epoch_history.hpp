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
#ifndef FOEDUS_LOG_EPOCH_HISTORY_HPP_
#define FOEDUS_LOG_EPOCH_HISTORY_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/epoch.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_id.hpp"

namespace foedus {
namespace log {
/**
 * @brief Represents an event where a logger switched its epoch.
 * @ingroup LOG
 * @details
 * This object is POD.
 */
struct EpochHistory {
  explicit EpochHistory(const EpochMarkerLogType& marker)
    : old_epoch_(marker.old_epoch_), new_epoch_(marker.new_epoch_),
    log_file_ordinal_(marker.log_file_ordinal_), log_file_offset_(marker.log_file_offset_) {
  }

  Epoch           old_epoch_;
  Epoch           new_epoch_;
  LogFileOrdinal  log_file_ordinal_;
  uint64_t        log_file_offset_;

  friend std::ostream& operator<<(std::ostream& o, const EpochHistory& v);
};

}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_EPOCH_HISTORY_HPP_
