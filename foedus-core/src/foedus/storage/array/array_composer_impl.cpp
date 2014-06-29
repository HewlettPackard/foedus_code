/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_composer_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"

namespace foedus {
namespace storage {
namespace array {

ArrayComposer::ArrayComposer(
    Engine *engine,
    const ArrayPartitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    const snapshot::Snapshot& new_snapshot)
  : engine_(engine),
    partitioner_(partitioner),
    snapshot_writer_(snapshot_writer),
    new_snapshot_(new_snapshot) {
  ASSERT_ND(partitioner);
}

ErrorStack ArrayComposer::compose(
  snapshot::SortedBuffer** log_streams,
  uint32_t log_streams_count,
  const memory::AlignedMemorySlice& work_memory) {
  VLOG(0) << to_string() << " composing with " << log_streams_count << " streams...";
  debugging::StopWatch stop_watch;

  // TODO(Hideaki): implement

  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

uint64_t ArrayComposer::get_required_work_memory_size(
  snapshot::SortedBuffer** log_streams,
  uint32_t log_streams_count) const {
  return 0;
}


void ArrayComposer::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<ArrayComposer>"
      << "<partitioner_>" << partitioner_ << "</partitioner_>"
      << "<snapshot_writer_>" << snapshot_writer_ << "</snapshot_writer_>"
      << "<new_snapshot>" << new_snapshot_ << "</new_snapshot>"
    << "</ArrayComposer>";
}

std::string ArrayComposer::to_string() const {
  return std::string("ArrayComposer:storage-") + std::to_string(partitioner_->get_storage_id())
    + std::string(":writer-") + snapshot_writer_->to_string();
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
