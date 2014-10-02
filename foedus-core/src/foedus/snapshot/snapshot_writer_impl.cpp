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
SnapshotWriter::SnapshotWriter(
  Engine* engine,
  uint16_t numa_node,
  SnapshotId snapshot_id,
  memory::AlignedMemory* pool_memory,
  memory::AlignedMemory* intermediate_memory,
  bool append)
  : engine_(engine),
  numa_node_(numa_node),
  append_(append),
  snapshot_id_(snapshot_id),
  pool_memory_(pool_memory),
  page_base_(reinterpret_cast<storage::Page*>(pool_memory_.get_block())),
  pool_size_(pool_memory_.get_size() / sizeof(storage::Page)),
  intermediate_memory_(intermediate_memory),
  intermediate_base_(reinterpret_cast<storage::Page*>(intermediate_memory_.get_block())),
  intermediate_size_(intermediate_memory_.get_size() / sizeof(storage::Page)),
  snapshot_file_(nullptr),
  next_page_id_(0) {
}

bool SnapshotWriter::close() {
  if (snapshot_file_) {
    bool success = true;
    fs::Path path = snapshot_file_->get_path();
    bool closed = snapshot_file_->close();
    if (!closed) {
      LOG(ERROR) << "Failed to close a snapshot file: " << *this;
      success = false;
    }
    // also fsync the file.
    if (!fs::fsync(path, true)) {
      LOG(ERROR) << "Failed to fsync a snapshot file: " << *this;
      success = false;
    }

    delete snapshot_file_;
    snapshot_file_ = nullptr;
    return success;
  } else {
    return true;
  }
}

fs::Path SnapshotWriter::get_snapshot_file_path() const {
  fs::Path path(engine_->get_options().snapshot_.construct_snapshot_file_path(snapshot_id_,
                                                                              numa_node_));
  return path;
}

ErrorStack SnapshotWriter::open() {
  close();
  fs::Path path(get_snapshot_file_path());
  snapshot_file_ = new fs::DirectIoFile(path, engine_->get_options().snapshot_.emulation_);
  bool create_new_file = append_ ? false : true;
  WRAP_ERROR_CODE(snapshot_file_->open(true, true, true, create_new_file));
  if (!append_) {
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
  } else {
    // if appending, nothing to do
    uint64_t file_size = snapshot_file_->get_current_offset();
    LOG(INFO) << to_string() << " Appending to " << file_size << "th bytes";
    ASSERT_ND(file_size >= sizeof(storage::Page));  // have at least the dummy page
    ASSERT_ND(file_size % sizeof(storage::Page) == 0);  // must be aligned writes
    uint64_t next_offset = file_size / sizeof(storage::Page);
    next_page_id_ = storage::to_snapshot_page_pointer(snapshot_id_, numa_node_, next_offset);
  }
  return kRetOk;
}

ErrorCode SnapshotWriter::dump_general(
  const memory::AlignedMemorySlice& slice,
  memory::PagePoolOffset from_page,
  uint32_t count) {
  ASSERT_ND(slice.get_size() / sizeof(storage::Page) >= from_page + count);
#ifndef NDEBUG
  storage::Page* base = reinterpret_cast<storage::Page*>(slice.get_block());
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
      slice,
      sizeof(storage::Page) * from_page,
      sizeof(storage::Page) * count)));
  next_page_id_ += count;
  return kErrorCodeOk;
}

std::ostream& operator<<(std::ostream& o, const SnapshotWriter& v) {
  o << "<SnapshotWriter>"
    << "<numa_node_>" << v.numa_node_ << "</numa_node_>"
    << "<append_>" << v.append_ << "</append_>"
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
