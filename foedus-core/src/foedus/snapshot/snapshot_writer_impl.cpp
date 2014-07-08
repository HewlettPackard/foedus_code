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
#include "foedus/fs/filesystem.hpp"
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
  page_base_(nullptr),
  pool_size_(0),
  intermediate_base_(nullptr),
  intermediate_size_(0),
  next_page_id_(0) {
}

bool SnapshotWriter::close() {
  fs::Path path = snapshot_file_->get_path();
  bool closed = snapshot_file_->close();
  if (!closed) {
    return false;
  }
  // also fsync the file.
  return fs::fsync(path, true);
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

  uint64_t intermediate_byte_size = static_cast<uint64_t>(
    engine_->get_options().snapshot_.snapshot_writer_intermediate_pool_size_mb_) << 20;
  intermediate_memory_.alloc(
    intermediate_byte_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_),
  intermediate_base_ = reinterpret_cast<storage::Page*>(intermediate_memory_.get_block());
  intermediate_size_ = intermediate_byte_size / sizeof(storage::Page);

  clear_snapshot_file();
  fs::Path path(get_snapshot_file_path());
  snapshot_file_ = new fs::DirectIoFile(path, engine_->get_options().snapshot_.emulation_);
  WRAP_ERROR_CODE(snapshot_file_->open(true, true, true, true));

  // write page-0. this is a dummy page which will never be read
  char* first_page = reinterpret_cast<char*>(pool_memory_.get_block());
  // first 8 bytes have some data, but we would use them just for sanity checks and debugging
  storage::PageHeader* first_page_header = reinterpret_cast<storage::PageHeader*>(first_page);
  first_page_header->page_id_ = storage::to_snapshot_page_pointer(snapshot_id_, numa_node_, 0);
  first_page_header->storage_id_ = 0x1BF0ED05;  // something unusual
  first_page_header->checksum_ = 0x1BF0ED05;  // something unusual
  std::memset(
    first_page + sizeof(storage::PageHeader),
    0,
    sizeof(storage::Page) - sizeof(storage::PageHeader));
  std::string duh("This is the first 4kb page of a snapshot file in libfoedus."
    " The first page is never used as data. It just has the common page header"
    " and these useless sentences. Maybe we put our complaints on our cafeteria here.");
  std::memcpy(first_page + sizeof(storage::PageHeader), duh.data(), duh.size());

  WRAP_ERROR_CODE(snapshot_file_->write(sizeof(storage::Page), pool_memory_));
  next_page_id_ = storage::to_snapshot_page_pointer(snapshot_id_, numa_node_, 1);
  return kRetOk;
}

ErrorStack SnapshotWriter::uninitialize_once() {
  ErrorStackBatch batch;
  intermediate_memory_.release_block();
  pool_memory_.release_block();
  clear_snapshot_file();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorCode SnapshotWriter::dump_general(
  memory::AlignedMemory* memory,
  memory::PagePoolOffset from_page,
  uint32_t count) {
#ifndef NDEBUG
  storage::Page* base = reinterpret_cast<storage::Page*>(memory->get_block());
  for (memory::PagePoolOffset i = 0; i < count; ++i) {
    storage::SnapshotLocalPageId correct_local_page_id = next_page_id_ + i;
    storage::SnapshotPagePointer correct_page_id
      = storage::to_snapshot_page_pointer(snapshot_id_, numa_node_, correct_local_page_id);
    storage::Page* page = base + from_page + i;
    ASSERT_ND(page->get_header().page_id_ == correct_page_id);
  }
#endif  // NDEBUG
  CHECK_ERROR_CODE(snapshot_file_->write(
    sizeof(storage::Page) * count,
    memory::AlignedMemorySlice(
      memory,
      sizeof(storage::Page) * from_page,
      sizeof(storage::Page) * count)));
  next_page_id_ += count;
  return kErrorCodeOk;
}

std::ostream& operator<<(std::ostream& o, const SnapshotWriter& v) {
  o << "<SnapshotWriter>"
    << "<numa_node_>" << v.numa_node_ << "</numa_node_>"
    << "<snapshot_id_>" << v.snapshot_id_ << "</snapshot_id_>"
    << "<pool_memory_>" << v.pool_memory_ << "</pool_memory_>"
    << "<pool_size_>" << v.pool_size_ << "</pool_size_>"
    << "<intermediate_memory_>" << v.intermediate_memory_ << "</intermediate_memory_>"
    << "<intermediate_size_>" << v.intermediate_size_ << "</intermediate_size_>"
    << "<next_page_id_>" << v.next_page_id_ << "</next_page_id_>"
    << "</SnapshotWriter>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
