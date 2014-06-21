/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
