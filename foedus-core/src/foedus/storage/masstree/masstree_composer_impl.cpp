/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_composer_impl.hpp"

#include <glog/logging.h>

#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"

namespace foedus {
namespace storage {
namespace masstree {

MasstreeComposer::MasstreeComposer(
    Engine *engine,
    StorageId storage_id,
    snapshot::SnapshotWriter* snapshot_writer,
    cache::SnapshotFileSet* previous_snapshot_files,
    const snapshot::Snapshot& new_snapshot)
  : Composer(engine, storage_id, snapshot_writer, previous_snapshot_files, new_snapshot) {
}
ErrorStack MasstreeComposer::compose(
  snapshot::SortedBuffer* const* /*log_streams*/,
  uint32_t /*log_streams_count*/,
  const memory::AlignedMemorySlice& /*work_memory*/,
  Page* /*root_info_page*/) {
  debugging::StopWatch stop_watch;
  stop_watch.stop();
  VLOG(0) << to_string() << " compose() done in " << stop_watch.elapsed_ms() << "ms. head page=";
  return kRetOk;
}

ErrorStack MasstreeComposer::construct_root(
  const Page* const* /*root_info_pages*/,
  uint32_t /*root_info_pages_count*/,
  const memory::AlignedMemorySlice& /*work_memory*/,
  SnapshotPagePointer* new_root_page_pointer) {
  debugging::StopWatch stop_watch;

  stop_watch.stop();
  VLOG(0) << to_string() << " construct_root() done in " << stop_watch.elapsed_us() << "us."
    << ". new root head page=" << assorted::Hex(*new_root_page_pointer);
  return kRetOk;
}

void MasstreeComposer::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<MasstreeComposer>"
      << "<snapshot_writer_>" << snapshot_writer_ << "</snapshot_writer_>"
      << "<new_snapshot>" << new_snapshot_ << "</new_snapshot>"
    << "</MasstreeComposer>";
}

std::string MasstreeComposer::to_string() const {
  return std::string("MasstreeComposer:storage-")
    + std::to_string(storage_id_);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
