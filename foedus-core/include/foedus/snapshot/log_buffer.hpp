/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_BUFFER_HPP_
#define FOEDUS_SNAPSHOT_LOG_BUFFER_HPP_

#include "foedus/log/common_log_types.hpp"
#include "foedus/snapshot/snapshot_id.hpp"

namespace foedus {
namespace snapshot {
/**
 * Packages handling of 4-bytes representation of position in log buffers.
 * @ingroup SNAPSHOT
 */
struct LogBuffer {
  explicit LogBuffer(char* base_address) : base_address_(base_address) {}
  char* const base_address_;

  inline log::RecordLogType* resolve(BufferPosition position) const {
    return reinterpret_cast<log::RecordLogType*>(
      base_address_ + from_buffer_position(position));
  }
  inline BufferPosition compact(const log::RecordLogType* address) const {
    return to_buffer_position(reinterpret_cast<const char*>(address) - base_address_);
  }
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_BUFFER_HPP_
