/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/log_buffer.hpp"

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

void InMemorySortedBuffer::describe(std::ostream* optr) const {
  std::ostream& o = *optr;
  o << "<InMemorySortedBuffer>"
      << "<block_>" << block_ << "</block_>"
      << "<cur_pos_>" << cur_pos_ << "</cur_pos_>"
      << "<end_pos_>" << end_pos_ << "</end_pos_>"
    << "</InMemorySortedBuffer>";
}


std::string DumpFileSortedBuffer::to_string() const {
  return std::string("DumpFileSortedBuffer: ") + file_->get_path().string();
}

void DumpFileSortedBuffer::describe(std::ostream* optr) const {
  std::ostream& o = *optr;
  o << "<DumpFileSortedBuffer>"
      << file_
      << "<io_buffer_>" << io_buffer_ << "</io_buffer_>"
      << "<cur_buffer_pos_>" << cur_buffer_pos_ << "</cur_buffer_pos_>"
      << "<cur_file_pos_>" << cur_file_pos_ << "</cur_file_pos_>"
      << "<end_file_pos_>" << end_file_pos_ << "</end_file_pos_>"
    << "</DumpFileSortedBuffer>";
}

ErrorCode DumpFileSortedBuffer::wind(uint64_t* cur_pos, uint64_t* end_pos) {
  const uint64_t old_pos = *cur_pos;
  const uint64_t buffer_size = io_buffer_.get_size();
  ASSERT_ND(old_pos >= cur_buffer_pos_);
  ASSERT_ND(file_size_ == fs::file_size(file_->get_path()));
  ASSERT_ND(cur_file_pos_ % kAlignment == 0);
  ASSERT_ND(old_pos <= buffer_size);
  ASSERT_ND(cur_file_pos_ + old_pos <= end_file_pos_);

  uint64_t read_to;
  if (buffer_size == old_pos) {
    *cur_pos = 0;
    read_to = 0;
    cur_file_pos_ += buffer_size;
  } else {
    // move the remaining (non-consumed) content in the buffer to the beginning of buffer.
    // we have to do this with alignment (this is why the returned cur_pos might not be zero)
    uint64_t move_from = old_pos / kAlignment * kAlignment;
    ASSERT_ND(move_from <= old_pos);
    ASSERT_ND(move_from > old_pos - kAlignment);
    char* buffer = reinterpret_cast<char*>(io_buffer_.get_block());
    std::memmove(buffer, buffer + move_from, buffer_size - move_from);
    *cur_pos -= move_from;
    read_to = kAlignment;
    cur_file_pos_ += move_from;
    ASSERT_ND(*cur_pos == old_pos % kAlignment);
  }
  cur_buffer_pos_ = *cur_pos;
  ASSERT_ND(cur_file_pos_ <= end_file_pos_);
  ASSERT_ND(cur_file_pos_ <= file_size_);
  ASSERT_ND(cur_file_pos_ % kAlignment == 0);

  // we read as much as possible. note that "cur_file_pos_ + buffer_size" might become
  // larger than end_file_pos_. It's fine, the next DumpFileSortedBuffer object for another
  // storage will take care of the excessive bytes.
  uint64_t read_upto_file_pos = std::min(cur_file_pos_ + buffer_size, file_size_);
  uint64_t desired_reads = read_upto_file_pos - (cur_file_pos_ + read_to);
  if (desired_reads > 0) {
    ErrorStack er = file_->read(desired_reads, io_buffer_);
    if (er.is_error()) {
      return er.get_error_code();
    }
  }

  *end_pos = std::min(buffer_size, end_file_pos_ - cur_file_pos_);
  ASSERT_ND(*end_pos >= *cur_pos);
  return kErrorCodeOk;
}

}  // namespace snapshot
}  // namespace foedus
