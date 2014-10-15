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
 *
 * @par In-memory and On-disk
 * There are two implementations of this buffer. InMemorySortedBuffer and DumpFileSortedBuffer.
 * See their comments for more details.
 *
 * @par Current block
 * The buffer logically conveys log entries of one storage because each composer is responsible for
 * only one storage. However, each sorted run file or in-memory buffer packs a bunch of storages.
 * So, each buffer conveys begin/end positions of the current storage block where the relevant
 * log entries start and end.
 *
 * @par Absolute and Relative positions
 * Finally, the buffer fundamentally has a limited size. It can't contain all the data at once.
 * Thus, we use absolute (byte position in entire file) and relative (byte position in buffer)
 * positions. In in-memory buffer, these two are the same.
 * To simplify, let "entire file size" or "file size" mean the file size in on-disk buffer,
 * the buffer size in in-memory buffer.
 *
 * @par Buffer window, offset, and byte positions
 */
class SortedBuffer {
 public:
  SortedBuffer(char* buffer, uint64_t buffer_size, uint64_t total_size)
    : buffer_(buffer),
      buffer_size_(buffer_size),
      offset_(0),
      total_size_(total_size),
      cur_block_storage_id_(0),
      cur_block_log_count_(0),
      cur_block_abosulte_begin_(0),
      cur_block_abosulte_end_(0) {
    assert_checks();
  }
  virtual ~SortedBuffer() {}

  uint64_t    to_relative_pos(uint64_t absolute_pos) const {
    ASSERT_ND(absolute_pos >= offset_);
    ASSERT_ND(absolute_pos <= offset_ + buffer_size_);
    return absolute_pos - offset_;
  }

  uint64_t    to_absolute_pos(uint64_t relative_pos) const {
    ASSERT_ND(relative_pos < buffer_size_);
    return relative_pos + offset_;
  }

  /** Returns the buffer memory. */
  const char* get_buffer() const { return buffer_; }

  /** Returns the size of buffer memory. */
  uint64_t    get_buffer_size() const { return buffer_size_; }

  /**
   * @brief Returns the absolute byte position of the buffer's beginning in the entire file.
   * @details
   * For example, buffer size = 1MB, file size = 1GB. Now we are reading 3200KB-4224KB region.
   * In that case, this returns 3200KB.
   * This value changes when the caller invokes wind() to shifts the buffer window.
   */
  uint64_t    get_offset() const { return offset_; }

  /** Returns the total size of the underlying stream (eg file size). */
  uint64_t    get_total_size() const { return total_size_; }

  storage::StorageId  get_cur_block_storage_id() const { return cur_block_storage_id_; }
  uint32_t            get_cur_block_log_count() const { return cur_block_log_count_; }

  /** Current storage block's beginning in absolute byte position in the file. */
  uint64_t            get_cur_block_abosulte_begin() const { return cur_block_abosulte_begin_; }
  /** Current storage block's end in absolute byte position in the file. */
  uint64_t            get_cur_block_abosulte_end() const { return cur_block_abosulte_end_; }

  void        set_current_block(
    storage::StorageId storage_id,
    uint32_t           log_count,
    uint64_t           abosulte_begin,
    uint64_t           abosulte_end) {
    cur_block_storage_id_ = storage_id;
    cur_block_log_count_ = log_count;
    cur_block_abosulte_begin_ = abosulte_begin;
    cur_block_abosulte_end_ = abosulte_end;
    assert_checks();
  }
  bool        is_valid_current_block() const { return cur_block_storage_id_ != 0; }
  void        invalidate_current_block() { cur_block_storage_id_ = 0; }

  /**
   * @brief Loads next data block to be consumed by the caller (composer).
   * @param[in] next_absolute_pos the absolute byte position that will be guaranteed to be
   * within the buffer after this method call. Note that this position might not become
   * 0-th byte position after this method call because the buffer might be backed by an aligned
   * File I/O.
   * @pre next_absolute_pos <= get_total_size()
   * @pre next_absolute_pos >= get_offset() : we allow foward-only iteration.
   * @pre next_absolute_pos <= get_offset() + get_buffer_size() : we allow contiguous-read only.
   * @post get_offset() + get_buffer_size() > next_absolute_pos >= get_offset()
   * @return File I/O related errors only
   */
  virtual ErrorCode     wind(uint64_t next_absolute_pos) = 0;

  /** Returns a short string that briefly describes this object. */
  virtual std::string   to_string() const = 0;

  /** Writes out a detailed description of this object to stream. */
  virtual void          describe(std::ostream* o) const = 0;

  friend std::ostream&  operator<<(std::ostream& o, const SortedBuffer& v);

  void assert_checks() {
    ASSERT_ND(offset_ <= total_size_);
    ASSERT_ND(offset_ <= buffer_size_);
    if (cur_block_storage_id_ != 0) {
      ASSERT_ND(cur_block_log_count_ > 0);
      ASSERT_ND(cur_block_abosulte_begin_ <= cur_block_abosulte_end_);
      ASSERT_ND(cur_block_abosulte_end_ <= total_size_);

      // did we overlook the begin/end of the block?
      ASSERT_ND(cur_block_abosulte_begin_ <= offset_ + buffer_size_);
      ASSERT_ND(cur_block_abosulte_end_ >= offset_);
    }
  }

 protected:
  char* const         buffer_;
  const uint64_t      buffer_size_;
  /** see get_offset() */
  uint64_t            offset_;
  /** see get_total_size() */
  const uint64_t      total_size_;

  /** If this is zero, this buffer is not set for reading any block. */
  storage::StorageId  cur_block_storage_id_;
  uint32_t            cur_block_log_count_;
  /** see get_cur_block_abosulte_begin() */
  uint64_t            cur_block_abosulte_begin_;
  /** see get_cur_block_abosulte_end() */
  uint64_t            cur_block_abosulte_end_;

  void describe_base_elements(std::ostream* o) const;
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
class InMemorySortedBuffer CXX11_FINAL : public SortedBuffer {
 public:
  InMemorySortedBuffer(char* buffer, uint64_t buffer_size)
    : SortedBuffer(buffer, buffer_size, buffer_size) {
  }
  ~InMemorySortedBuffer() CXX11_OVERRIDE {}

  std::string to_string() const CXX11_OVERRIDE { return "<in-memory buffer>"; }
  ErrorCode   wind(uint64_t next_absolute_pos) CXX11_OVERRIDE {
    ASSERT_ND(next_absolute_pos <= buffer_size_);
    return kErrorCodeOk;
  }
  void        describe(std::ostream* o) const CXX11_OVERRIDE;
};

/**
 * @brief Implementation of SortedBuffer that is backed by a dumped file.
 * @ingroup SNAPSHOT
 * @details
 * When the reducer's buffer becomes full, it sorts all entries in it and dumps it to a file
 * (sorted run file). This stream sequentially reads from the file.
 */
class DumpFileSortedBuffer CXX11_FINAL : public SortedBuffer {
 public:
  /**
   * wraps the given file as a buffer. note that this object does \b NOT take ownership of
   * anything given to this constructor. it doesn't delete/open file or memory. it's caller's
   * responsibility (in other words, this object is just a view. so is InMemorySortedBuffer).
   */
  DumpFileSortedBuffer(fs::DirectIoFile *file, memory::AlignedMemorySlice io_buffer);
  ~DumpFileSortedBuffer() CXX11_OVERRIDE {}

  std::string to_string() const CXX11_OVERRIDE;
  ErrorCode   wind(uint64_t next_absolute_pos) CXX11_OVERRIDE;
  void        describe(std::ostream* o) const CXX11_OVERRIDE;

  fs::DirectIoFile*                 get_file()      const { return file_; }
  const memory::AlignedMemorySlice& get_io_buffer() const { return io_buffer_; }

 private:
  enum Constants {
    kAlignment = 1 << 12,
  };
  fs::DirectIoFile* const           file_;
  memory::AlignedMemorySlice const  io_buffer_;
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_BUFFER_HPP_
