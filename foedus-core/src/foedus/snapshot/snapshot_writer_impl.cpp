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
  pool_size_(0),
  dump_io_buffer_(nullptr),
  allocated_pages_(0),
  dumped_pages_(0) {
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
  clear_snapshot_file();
  fs::Path path(get_snapshot_file_path());
  snapshot_file_ = new fs::DirectIoFile(path, engine_->get_options().snapshot_.emulation_);
  WRAP_ERROR_CODE(snapshot_file_->open(true, true, true, true));

  // write page-0. this is a dummy page which will never be read
  char* first_page = reinterpret_cast<char*>(pool_memory_.get_block());
  // first 8 bytes have some data, but we would use them just for sanity checks and debugging
  *reinterpret_cast<uint32_t*>(first_page) = 0x1BF0ED05;
  *reinterpret_cast<uint16_t*>(first_page + 4) = get_snapshot_id();
  *reinterpret_cast<uint16_t*>(first_page + 6) = get_numa_node();
  std::memset(first_page + 8, 0, sizeof(storage::Page) - 8);
  std::string duh("This is the first 4kb page of a snapshot file in libfoedus."
    " The first page is never used as data. It just consists of"
    " first magic word (0x1BF0ED05), snapshot ID (2 bytes), then partition (NUMA node, 2 bytes),"
    " and these useless sentences. Maybe we put our complaints on our cafeteria here.");
  std::memcpy(first_page + 8, duh.data(), duh.size());

  WRAP_ERROR_CODE(snapshot_file_->write(sizeof(storage::Page), pool_memory_));
  allocated_pages_ = 1;
  dumped_pages_ = 1;
  return kRetOk;
}

ErrorStack SnapshotWriter::uninitialize_once() {
  ErrorStackBatch batch;
  pool_memory_.release_block();
  clear_snapshot_file();
  return SUMMARIZE_ERROR_BATCH(batch);
}

inline uint32_t count_contiguous(const memory::PagePoolOffset* array, uint32_t from, uint32_t to) {
  uint32_t contiguous = 1;
  for (memory::PagePoolOffset value = array[from];
        from + contiguous < to && value == array[from + contiguous];
        ++contiguous) {
    continue;
  }
  return contiguous;
}

ErrorCode SnapshotWriter::dump_pages(const memory::PagePoolOffset* memory_pages, uint32_t count) {
  ASSERT_ND(dump_io_buffer_);
  // to avoid writing out small pieces, we copy the pages to dump_io_buffer_.
  storage::Page* buffer = reinterpret_cast<storage::Page*>(dump_io_buffer_->get_block());
  const uint64_t max_buffered = dump_io_buffer_->get_size() / sizeof(storage::Page);
  uint32_t buffered = 0;
  uint32_t cur = 0;
  while (cur < count) {
    uint32_t contiguous = count_contiguous(memory_pages, cur, count);

    // flush the buffer if full
    if (buffered + contiguous > max_buffered) {
      CHECK_ERROR_CODE(snapshot_file_->write(
        sizeof(storage::Page) * buffered,
        memory::AlignedMemorySlice(dump_io_buffer_, 0, sizeof(storage::Page) * buffered)));
      buffered = 0;
    }

    // copy the pages to IO buffer. this might be wasteful when contiguous is large
    // (we can directly write from there). But, then the client should have used another method.
    storage::Page* source = resolve(memory_pages[cur]);
    std::memcpy(buffer + buffered, source, sizeof(storage::Page) * contiguous);
    buffered += contiguous;
    cur += contiguous;
  }

  if (buffered > 0) {
    CHECK_ERROR_CODE(snapshot_file_->write(
      sizeof(storage::Page) * buffered,
      memory::AlignedMemorySlice(dump_io_buffer_, 0, sizeof(storage::Page) * buffered)));
  }

  dumped_pages_ += count;
  return kErrorCodeOk;
}


ErrorCode SnapshotWriter::dump_pages(memory::PagePoolOffset from_page, uint32_t count) {
  // the data are already sequetial. no need for copying
  CHECK_ERROR_CODE(snapshot_file_->write(
    sizeof(storage::Page) * count,
    memory::AlignedMemorySlice(
      &pool_memory_,
      sizeof(storage::Page) * from_page,
      sizeof(storage::Page) * count)));
  dumped_pages_ += count;
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
  allocated_pages_ = kNewOffsetBase + excluded_count;
  return kNewOffsetBase;
}


std::ostream& operator<<(std::ostream& o, const SnapshotWriter& v) {
  o << "<SnapshotWriter>"
    << "<numa_node_>" << v.numa_node_ << "</numa_node_>"
    << "<snapshot_id_>" << v.snapshot_id_ << "</snapshot_id_>"
    << "<pool_memory_>" << v.pool_memory_ << "</pool_memory_>"
    << "<pool_size_>" << v.pool_size_ << "</pool_size_>"
    << "<allocated_pages_>" << v.allocated_pages_ << "</allocated_pages_>"
    << "<dumped_pages_>" << v.dumped_pages_ << "</dumped_pages_>"
    << "</SnapshotWriter>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
