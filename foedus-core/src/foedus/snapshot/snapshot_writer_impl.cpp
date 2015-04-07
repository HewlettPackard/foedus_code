/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
  intermediate_memory_(intermediate_memory),
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
    char* first_page = reinterpret_cast<char*>(pool_memory_->get_block());
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

    WRAP_ERROR_CODE(snapshot_file_->write(sizeof(storage::Page), *pool_memory_));
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
  memory::AlignedMemory* buffer,
  memory::PagePoolOffset from_page,
  uint32_t count) {
  ASSERT_ND(buffer->get_size() / sizeof(storage::Page) >= from_page + count);
#ifndef NDEBUG
  storage::Page* base = reinterpret_cast<storage::Page*>(buffer->get_block());
  for (memory::PagePoolOffset i = 0; i < count; ++i) {
    storage::SnapshotLocalPageId correct_local_page_id
      = storage::extract_local_page_id_from_snapshot_pointer(next_page_id_) + i;
    storage::Page* page = base + from_page + i;
    uint64_t page_id = page->get_header().page_id_;
    storage::SnapshotLocalPageId local_page_id
      = storage::extract_local_page_id_from_snapshot_pointer(page_id);
    uint16_t node = storage::extract_numa_node_from_snapshot_pointer(page_id);
    uint16_t snapshot_id = storage::extract_snapshot_id_from_snapshot_pointer(page_id);
    ASSERT_ND(local_page_id == correct_local_page_id);
    ASSERT_ND(node == numa_node_);
    ASSERT_ND(snapshot_id == snapshot_id_);
  }
#endif  // NDEBUG
  CHECK_ERROR_CODE(snapshot_file_->write(
    sizeof(storage::Page) * count,
    memory::AlignedMemorySlice(
      buffer,
      sizeof(storage::Page) * from_page,
      sizeof(storage::Page) * count)));
  next_page_id_ += count;
  return kErrorCodeOk;
}

ErrorCode SnapshotWriter::expand_pool_memory(uint32_t required_pages, bool retain_content) {
  if (required_pages <= get_page_size()) {
    return kErrorCodeOk;
  }

  uint64_t bytes = required_pages * sizeof(storage::Page);
  CHECK_ERROR_CODE(pool_memory_->assure_capacity(bytes, 2.0, retain_content));
  ASSERT_ND(get_page_size() >= required_pages);
  return kErrorCodeOk;
}



ErrorCode SnapshotWriter::expand_intermediate_memory(uint32_t required_pages, bool retain_content) {
  if (required_pages <= get_intermediate_size()) {
    return kErrorCodeOk;
  }

  uint64_t bytes = required_pages * sizeof(storage::Page);
  CHECK_ERROR_CODE(intermediate_memory_->assure_capacity(bytes, 2.0, retain_content));
  ASSERT_ND(get_intermediate_size() >= required_pages);
  return kErrorCodeOk;
}

std::ostream& operator<<(std::ostream& o, const SnapshotWriter& v) {
  o << "<SnapshotWriter>"
    << "<numa_node_>" << v.numa_node_ << "</numa_node_>"
    << "<append_>" << v.append_ << "</append_>"
    << "<snapshot_id_>" << v.snapshot_id_ << "</snapshot_id_>"
    << "<pool_memory_>" << *v.pool_memory_ << "</pool_memory_>"
    << "<intermediate_memory_>" << *v.intermediate_memory_ << "</intermediate_memory_>"
    << "<next_page_id_>" << v.next_page_id_ << "</next_page_id_>"
    << "</SnapshotWriter>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
