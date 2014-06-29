/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot_writer_impl.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/snapshot/log_gleaner_impl.hpp"
#include "foedus/snapshot/log_reducer_impl.hpp"
#include "foedus/snapshot/snapshot.hpp"

namespace foedus {
namespace snapshot {
SnapshotWriter::SnapshotWriter(Engine* engine, LogReducer* parent)
  : engine_(engine),
  parent_(parent),
  numa_node_(parent->get_id()),
  snapshot_id_(parent_->get_parent()->get_snapshot()->id_),
  snapshot_file_(nullptr),
  page_pool_(
    static_cast<uint64_t>(engine->get_options().snapshot_.snapshot_writer_page_pool_size_mb_) << 20,
    memory::kHugepageSize,
    numa_node_),
  page_resolver_(page_pool_.get_resolver()),
  dump_io_buffer_(nullptr),
  fixed_upto_(0) {
}

void SnapshotWriter::clear_snapshot_file() {
  if (snapshot_file_) {
    snapshot_file_->close();
    delete snapshot_file_;
    snapshot_file_ = nullptr;
  }
}

fs::Path SnapshotWriter::get_snapshot_file_path() const {
  std::string folder = engine_->get_options().snapshot_.convert_folder_path_pattern(numa_node_);
  // snapshot_<snapshot-id>_node_<node-id>.data
  std::string name =
    std::string("snapshot_")
    + std::to_string(snapshot_id_)
    + std::string("_")
    + std::to_string(static_cast<int>(numa_node_));
  fs::Path path(folder);
  path /= name;
  return path;
}

ErrorStack SnapshotWriter::initialize_once() {
  CHECK_ERROR(page_pool_.initialize());
  page_resolver_ = page_pool_.get_resolver();
  clear_snapshot_file();
  fs::Path path(get_snapshot_file_path());
  snapshot_file_ = new fs::DirectIoFile(path, engine_->get_options().snapshot_.emulation_);
  CHECK_ERROR(snapshot_file_->open(true, true, true, true));
  return kRetOk;
}

ErrorStack SnapshotWriter::uninitialize_once() {
  ErrorStackBatch batch;
  batch.emprace_back(page_pool_.uninitialize());
  clear_snapshot_file();
  return SUMMARIZE_ERROR_BATCH(batch);
}

storage::SnapshotLocalPageId SnapshotWriter::fix_pages(
  const memory::PagePoolOffset* memory_pages,
  uint32_t count) {
}


ErrorCode SnapshotWriter::dump_pages(
  const memory::PagePoolOffset* memory_pages,
  uint32_t count) {
  return kErrorCodeOk;
}


void SnapshotWriter::reset_pool(
  const memory::PagePoolOffset* excluded_pages,
  uint32_t excluded_count) {
}


std::ostream& operator<<(std::ostream& o, const SnapshotWriter& v) {
  o << "<SnapshotWriter>"
    << "<numa_node_>" << v.numa_node_ << "</numa_node_>"
    << "<snapshot_id_>" << v.snapshot_id_ << "</snapshot_id_>"
    << "<page_pool_>" << v.page_pool_ << "</page_pool_>"
    << "</SnapshotWriter>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
