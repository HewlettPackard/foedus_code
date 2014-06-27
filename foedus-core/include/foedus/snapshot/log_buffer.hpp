/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_BUFFER_HPP_
#define FOEDUS_SNAPSHOT_LOG_BUFFER_HPP_

#include <iosfwd>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/snapshot_id.hpp"

namespace foedus {
namespace snapshot {
/**
 * Packages handling of 4-bytes representation of position in log buffers.
 * @ingroup SNAPSHOT
 */
struct LogBuffer {
  explicit LogBuffer(char* base_address) : base_address_(base_address) {}
  char* const base_address_;

  inline log::RecordLogType* resolve(BufferPosition position) const {
    return reinterpret_cast<log::RecordLogType*>(
      base_address_ + from_buffer_position(position));
  }
  inline BufferPosition compact(const log::RecordLogType* address) const {
    return to_buffer_position(reinterpret_cast<const char*>(address) - base_address_);
  }
};

/**
 * @brief Represents one input stream of sorted log entries.
 * @ingroup SNAPSHOT
 * @details
 * At the end of reducer, the reducer reads dumped sorted run files and
 * does the final \b apply phase to construct the new version of each storage's pages.
 * This is done by invoking each storage's composer interface with this object as inputs.
 * To avoid invoking composer for each log entry (which is very expensive),
 * this object allows the composer to receive sufficiently large buffers and do sort-merge
 * itself.
 */
class SortedBuffer {
 public:
  virtual ~SortedBuffer() {}

  /**
   * @brief Loads next data block to be consumed by the caller (composer).
   * @param[in,out] cur_pos [in] the byte position relative to the memory upto which
   * the caller has consumed. [out] the byte position relative to the memory from which
   * the caller should resume consuming. Note that [out] might not be zero because the buffer
   * might be backed by an aligned File I/O.
   * @param[out] end_pos the byte position relative to the memory upto which the caller can
   * consume. if cur_pos == end_pos, it indicates that there is no more data to read.
   * @return File I/O related errors only
   */
  virtual ErrorCode   wind(uint64_t *cur_pos, uint64_t *end_pos) = 0;

  /** Returns the current byte position relative to the memory. */
  virtual uint64_t    get_cur_pos() const = 0;

  /** Returns the buffer memory. */
  virtual const char* get_block() const = 0;

  /** Returns the size of buffer memory. */
  virtual uint64_t    get_block_size() const = 0;

  /** Returns a short string that briefly describes this object. */
  virtual std::string to_string() const = 0;

  /** Writes out a detailed description of this object to stream. */
  virtual void        describe(std::ostream* o) const = 0;

  friend std::ostream&    operator<<(std::ostream& o, const SortedBuffer& v);
};

/**
 * @brief Implementation of SortedBuffer that is backed by fully in-memory buffer.
 * @ingroup SNAPSHOT
 * @details
 * After all mappers are completed, the reducer actually skips dumping the last buffer
 * because anyway it needs to read it back (BTW, if this is the only buffer, everything becomes
 * in-memory, so efficient). In that case, we just sort the last buffer and provides it
 * to composers as it is. This object is used in such cases.
 */
class InMemorySortedBuffer CXX11_FINAL : public virtual SortedBuffer {
 public:
  InMemorySortedBuffer(const char* block, uint64_t begin_pos, uint64_t end_pos)
    : block_(block), cur_pos_(begin_pos), end_pos_(end_pos) {
      ASSERT_ND(cur_pos_ <= end_pos_);
    }
  ~InMemorySortedBuffer() CXX11_OVERRIDE {}

  uint64_t    get_cur_pos() const CXX11_OVERRIDE { return cur_pos_; }
  const char* get_block() const CXX11_OVERRIDE { return block_; }
  uint64_t    get_block_size() const CXX11_OVERRIDE { return end_pos_; }
  std::string to_string() const CXX11_OVERRIDE { return "<in-memory buffer>"; }
  ErrorCode   wind(uint64_t* cur_pos, uint64_t* end_pos) CXX11_OVERRIDE {
    ASSERT_ND(*cur_pos >= cur_pos_);
    ASSERT_ND(*cur_pos <= end_pos_);
    cur_pos_ = *cur_pos;
    *end_pos = end_pos_;
    return kErrorCodeOk;
  }
  void        describe(std::ostream* o) const CXX11_OVERRIDE;

 private:
  const char* const block_;
  uint64_t          cur_pos_;
  const uint64_t    end_pos_;
};

/**
 * @brief Implementation of SortedBuffer that is backed by a dumped file.
 * @ingroup SNAPSHOT
 * @details
 * When the reducer's buffer becomes full, it sorts all entries in it and dumps it to a file
 * (sorted run file). This stream sequentially reads from the file.
 */
class DumpFileSortedBuffer CXX11_FINAL : public virtual SortedBuffer {
 public:
  /**
   * wraps the given file as a buffer. note that this object does \b NOT take ownership of
   * anything given to this constructor. it doesn't delete/open file or memory. it's caller's
   * responsibility (which is good because the caller can cheaply instantiate many of this objects).
   */
  DumpFileSortedBuffer(
    fs::DirectIoFile *file,
    memory::AlignedMemorySlice io_buffer,
    uint64_t begin_buffer_pos,
    uint64_t begin_file_pos,
    uint64_t end_file_pos,
    uint64_t file_size)
    : file_(file),
    io_buffer_(io_buffer),
    cur_buffer_pos_(begin_buffer_pos),
    cur_file_pos_(begin_file_pos),
    end_file_pos_(end_file_pos),
    file_size_(file_size) {
    ASSERT_ND(cur_buffer_pos_ <= io_buffer_.get_size());
    ASSERT_ND(cur_file_pos_ + cur_buffer_pos_ <= end_file_pos);
    ASSERT_ND(end_file_pos <= file_size);
    // these must be aligned
    ASSERT_ND(cur_file_pos_ % kAlignment == 0);
    ASSERT_ND(end_file_pos_ % kAlignment == 0);
    ASSERT_ND(file_size_ % kAlignment == 0);
  }
  ~DumpFileSortedBuffer() CXX11_OVERRIDE {}

  uint64_t    get_cur_pos() const CXX11_OVERRIDE { return cur_buffer_pos_; }
  const char* get_block() const CXX11_OVERRIDE {
    return reinterpret_cast<const char*>(io_buffer_.get_block());
  }
  uint64_t    get_block_size() const CXX11_OVERRIDE {
    return io_buffer_.get_size();
  }
  std::string to_string() const CXX11_OVERRIDE;
  ErrorCode   wind(uint64_t* cur_pos, uint64_t* end_pos) CXX11_OVERRIDE;
  void        describe(std::ostream* o) const CXX11_OVERRIDE;

  uint64_t    get_cur_file_pos() const { return cur_file_pos_; }
  uint64_t    get_end_file_pos() const { return end_file_pos_; }

 private:
  enum Constants {
    kAlignment = 1 << 12,
  };
  fs::DirectIoFile *file_;
  memory::AlignedMemorySlice io_buffer_;
  uint64_t          cur_buffer_pos_;
  /** Byte offset in file corresponding to 1st byte in io_buffer_. */
  uint64_t          cur_file_pos_;
  const uint64_t    end_file_pos_;
  const uint64_t    file_size_;
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_BUFFER_HPP_
