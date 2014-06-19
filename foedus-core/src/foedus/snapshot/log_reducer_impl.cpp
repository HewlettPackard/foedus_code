/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <foedus/snapshot/log_reducer_impl.hpp>
#include <glog/logging.h>
#include <ostream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogReducer::handle_initialize() {
  const SnapshotOptions& option = engine_->get_options().snapshot_;

  uint64_t buffer_size = static_cast<uint64_t>(option.log_reducer_buffer_mb_) << 20;
  buffer_.alloc(
    buffer_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  return kRetOk;
}

ErrorStack LogReducer::handle_uninitialize() {
  ErrorStackBatch batch;
  buffer_.release_block();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack LogReducer::handle_process() {
  SPINLOCK_WHILE(!parent_->is_all_mappers_completed()) {
    WRAP_ERROR_CODE(check_cancelled());
  }
  return kRetOk;
}

std::ostream& operator<<(std::ostream& o, const LogReducer& v) {
  o << "<LogReducer>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<thread_>" << v.thread_ << "</thread_>"
    << "</LogReducer>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
