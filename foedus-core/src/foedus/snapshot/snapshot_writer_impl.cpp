/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot_writer_impl.hpp"

#include <stdint.h>
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
  next_page_(0),
  pool_size_(0),
  dump_io_buffer_(nullptr),
  fixed_upto_(0),
  dumped_upto_(0) {
}

bool SnapshotWriter::close() {
  return snapshot_file_->close();
}

void SnapshotWriter::clear_snapshot_file() {
  if (snapshot_file_) {
    close();
    delete snapshot_file_;
    snapshot_file_ = nullptr;
  }
}

fs::Path SnapshotWriter::get_snapshot_file_path() const {
  fs::Path path(engine_->get_options().snapshot_.construct_snapshot_file_path(snapshot_id_,
                                                                              numa_node_));
  return path;
}

ErrorStack SnapshotWriter::initialize_once() {
  uint64_t pool_byte_size = static_cast<uint64_t>(
    engine_->get_options().snapshot_.snapshot_writer_page_pool_size_mb_) << 20;
  pool_memory_.alloc(
    pool_byte_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_),
  page_base_ = reinterpret_cast<storage::Page*>(pool_memory_.get_block());
  pool_size_ = pool_byte_size / sizeof(storage::Page);
  next_page_ = 1;  // skip page-0.
  clear_snapshot_file();
  fs::Path path(get_snapshot_file_path());
  snapshot_file_ = new fs::DirectIoFile(path, engine_->get_options().snapshot_.emulation_);
  CHECK_ERROR(snapshot_file_->open(true, true, true, true));
  fixed_upto_ = 0;
  dumped_upto_ = 0;
  return kRetOk;
}

ErrorStack SnapshotWriter::uninitialize_once() {
  ErrorStackBatch batch;
  pool_memory_.release_block();
  clear_snapshot_file();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorCode SnapshotWriter::dump_pages(const memory::PagePoolOffset* memory_pages, uint32_t count) {
  dumped_upto_ += count;
  ASSERT_ND(dumped_upto_ == fixed_upto_);
  return kErrorCodeOk;
}


memory::PagePoolOffset SnapshotWriter::reset_pool(
  const memory::PagePoolOffset* excluded_pages,
  uint32_t excluded_count) {
  // move the excluded pages to the beginning.
  // Here, we asssume that excluded_pages are sorted by offset in ascending order.
  // Thus, the following memcpy is always safe (source page won't be overwritten)
  const memory::PagePoolOffset kNewOffsetBase = 1;
  for (uint32_t i = 0; i < excluded_count; ++i) {
    ASSERT_ND(i == 0 || excluded_pages[i] > excluded_pages[i - 1]);  // sorted?
    if (excluded_pages[i] != kNewOffsetBase + i) {
      std::memcpy(resolve(kNewOffsetBase + i), resolve(excluded_pages[i]), sizeof(storage::Page));
    }
  }
  next_page_ = kNewOffsetBase + excluded_count;
  return kNewOffsetBase;
}


std::ostream& operator<<(std::ostream& o, const SnapshotWriter& v) {
  o << "<SnapshotWriter>"
    << "<numa_node_>" << v.numa_node_ << "</numa_node_>"
    << "<snapshot_id_>" << v.snapshot_id_ << "</snapshot_id_>"
    << "<pool_memory_>" << v.pool_memory_ << "</pool_memory_>"
    << "<next_page_>" << v.next_page_ << "</next_page_>"
    << "<pool_size_>" << v.pool_size_ << "</pool_size_>"
    << "</SnapshotWriter>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
