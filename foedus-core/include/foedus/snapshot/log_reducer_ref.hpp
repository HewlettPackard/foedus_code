/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_REDUCER_REF_HPP_
#define FOEDUS_SNAPSHOT_LOG_REDUCER_REF_HPP_

#include <stdint.h>

#include <cstring>
#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace snapshot {

/**
 * A remote view of LogReducer from all engines.
 * @ingroup SNAPSHOT
 */
class LogReducerRef {
 public:
  LogReducerRef() {
    engine_ = CXX11_NULLPTR;
    control_block_ = CXX11_NULLPTR;
    buffers_[0] = CXX11_NULLPTR;
    buffers_[1] = CXX11_NULLPTR;
    root_info_pages_ = CXX11_NULLPTR;
  }
  LogReducerRef(Engine* engine, uint16_t node);

  uint16_t    get_id() const;
  std::string to_string() const;
  void        clear();
  uint32_t    get_total_storage_count() const;
  storage::Page* get_root_info_pages() { return root_info_pages_; }
  friend std::ostream&    operator<<(std::ostream& o, const LogReducerRef& v);

  /**
   * @brief Append the log entries of one storage in the given buffer to this reducer's buffer.
   * @param[in] storage_id all log entries are of this storage
   * @param[in] send_buffer contains log entries to copy
   * @param[in] log_count number of log entries to copy
   * @param[in] send_buffer_size byte count to copy
   * @details
   * This is the interface via which mappers send log entries to reducers.
   * Internally, this atomically changes the status of the current reducer buffer to reserve
   * a contiguous space and then copy without blocking other mappers.
   * If this methods hits a situation where the current buffer becomes full, this methods
   * wakes up the reducer and lets it switch the current buffer.
   * All log entries are contiguously copied. One block doesn't span two buffers.
   */
  void append_log_chunk(
    storage::StorageId storage_id,
    const char* send_buffer,
    uint32_t log_count,
    uint64_t send_buffer_size);

 protected:
  uint64_t  get_buffer_size() const;
  uint32_t  get_current_buffer_index_atomic() const;
  void*     get_buffer(uint32_t index) const;

  Engine*                 engine_;
  LogReducerControlBlock* control_block_;
  void*                   buffers_[2];
  storage::Page*          root_info_pages_;
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_REDUCER_REF_HPP_
