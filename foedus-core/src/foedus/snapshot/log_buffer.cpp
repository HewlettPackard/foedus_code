/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/log_buffer.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <ostream>
#include <string>

#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"

namespace foedus {
namespace snapshot {
std::ostream& operator<<(std::ostream& o, const SortedBuffer& v) {
  v.describe(&o);
  return o;
}

void SortedBuffer::describe_base_elements(std::ostream* optr) const {
  std::ostream& o = *optr;
  o << "<buffer_>" << reinterpret_cast<const void*>(buffer_) << "</buffer_>"
    << "<buffer_size_>" << buffer_size_ << "</buffer_size_>"
    << "<offset_>" << offset_ << "</offset_>"
    << "<total_size_>" << total_size_ << "</total_size_>"
    << "<cur_block_storage_id_>" << cur_block_storage_id_ << "</cur_block_storage_id_>"
    << "<cur_block_log_count_>" << cur_block_log_count_ << "</cur_block_log_count_>"
    << "<cur_block_abosulte_begin_>" << cur_block_abosulte_begin_ << "</cur_block_abosulte_begin_>"
    << "<cur_block_abosulte_end_>" << cur_block_abosulte_end_ << "</cur_block_abosulte_end_>";
}

void InMemorySortedBuffer::describe(std::ostream* optr) const {
  std::ostream& o = *optr;
  o << "<InMemorySortedBuffer>";
  describe_base_elements(optr);
  o << "</InMemorySortedBuffer>";
}


DumpFileSortedBuffer::DumpFileSortedBuffer(
  fs::DirectIoFile* file, memory::AlignedMemorySlice io_buffer)
  : SortedBuffer(
    reinterpret_cast<char*>(io_buffer.get_block()),
    io_buffer.get_size(),
    fs::file_size(file->get_path())),
    file_(file),
    io_buffer_(io_buffer) {
  ASSERT_ND(buffer_size_ % kAlignment == 0);
  ASSERT_ND(total_size_ % kAlignment == 0);
}

std::string DumpFileSortedBuffer::to_string() const {
  return std::string("DumpFileSortedBuffer: ") + file_->get_path().string();
}

void DumpFileSortedBuffer::describe(std::ostream* optr) const {
  std::ostream& o = *optr;
  o << "<DumpFileSortedBuffer>";
  describe_base_elements(optr);
  o << "<file_>" << file_ << "</file_>";
  o << "<io_buffer_>" << io_buffer_ << "</io_buffer_>";
  o << "</DumpFileSortedBuffer>";
}

ErrorCode DumpFileSortedBuffer::wind(uint64_t next_absolute_pos) {
  ASSERT_ND(offset_ % kAlignment == 0);
  assert_checks();
  if (next_absolute_pos == offset_ || offset_ + buffer_size_ >= total_size_) {
    return kErrorCodeOk;  // nothing to do then
  } else if (next_absolute_pos < offset_ ||  // backward wind
    next_absolute_pos >= offset_ + buffer_size_ ||  // jumps more than one buffer
    next_absolute_pos >= total_size_) {  // jumps exceeding the end of file
    // this class doesn't support these operations. We shouldn't need them in any case.
    LOG(FATAL) << " wtf next_absolute_pos=" << next_absolute_pos << ", offset=" << offset_
      << ", buffer_size=" << buffer_size_ << ", total_size_=" << total_size_;
    return kErrorCodeInvalidParameter;
  }

  // suppose buf=64M and we have read second window(64M-128M) and now moving on to third window.
  // in the easiest case, current offset_=64M, next_absolute_pos=128M. we just read 64M.
  // but, probably next_absolute_pos=128M-alpha, further alpha might not be 4k-aligned.
  // the following code takes care of those cases.
  ASSERT_ND(file_->get_current_offset() == offset_ + buffer_size_);
  uint64_t retained_bytes
    = assorted::align<uint64_t, kAlignment>(offset_ + buffer_size_ - next_absolute_pos);
  ASSERT_ND(retained_bytes <= buffer_size_);
  std::memmove(buffer_, buffer_ + buffer_size_ - retained_bytes, retained_bytes);

  // we read as much as possible. note that "next_absolute_pos + buffer_size_" might become larger
  // than cur_block_abosulte_end_. It's fine, the next DumpFileSortedBuffer object for another
  // storage will take care (and make use) of the excessive bytes.
  uint64_t desired_reads = std::min(
    buffer_size_ - retained_bytes,
    total_size_ - (offset_ + buffer_size_));
  memory::AlignedMemorySlice sub_slice(io_buffer_, retained_bytes, desired_reads);
  CHECK_ERROR_CODE(file_->read(desired_reads, sub_slice));
  offset_ = offset_ + buffer_size_ - retained_bytes;

  ASSERT_ND(offset_ % kAlignment == 0);
  ASSERT_ND(next_absolute_pos >= offset_);
  assert_checks();
  return kErrorCodeOk;
}

}  // namespace snapshot
}  // namespace foedus
