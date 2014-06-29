/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot_writer_impl.hpp"

#include <glog/logging.h>

#include "foedus/error_stack_batch.hpp"
#include "foedus/snapshot/log_reducer_impl.hpp"

namespace foedus {
namespace snapshot {
SnapshotWriter::SnapshotWriter(Engine* engine, LogReducer* parent)
  : engine_(engine), parent_(parent), id_(parent->get_id()) {
}


ErrorStack SnapshotWriter::initialize_once() {
  return kRetOk;
}

ErrorStack SnapshotWriter::uninitialize_once() {
  ErrorStackBatch batch;
  return SUMMARIZE_ERROR_BATCH(batch);
}

std::ostream& operator<<(std::ostream& o, const SnapshotWriter& v) {
  o << "<SnapshotWriter>"
    << "<id_>" << v.id_ << "</id_>"
    << "</SnapshotWriter>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
