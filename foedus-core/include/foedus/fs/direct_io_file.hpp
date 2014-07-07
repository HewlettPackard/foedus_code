/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_FS_DIRECT_IO_FILE_HPP_
#define FOEDUS_FS_DIRECT_IO_FILE_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fs/device_emulation_options.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/memory/fwd.hpp"

namespace foedus {
namespace fs {

/**
 * @brief Represents an I/O stream on one file without filesystem caching.
 * @ingroup FILESYSTEM
 * @details
 * This class bypasses filesystem caching for disk I/O for three purposes.
 *  \li Performance, as DBMS has better knowledge of disk accesses than OS.
 *  \li Memory footprint, as we might end up doubled memory consumption.
 *  \li Durability, as write-cache would undermine the synchronous flush during commit.
 *
 * This class is used for two kinds of files.
 *  \li Data file, or snapshot files.
 *  \li Log file
 * @todo Support Windows. MUCH later.
 */
class DirectIoFile {
 public:
  /** Represents low-level file descriptor. */
  typedef int file_descriptor;
  /** Constant values. */
  enum Constants {
    /** POSIX open() semantics says -1 is invalid or not-yet-opened. */
    kInvalidDescriptor = -1,
  };

  /** Analogue of SEEK_SET/SEEK_CUR/SEEK_END in POSIX. */
  enum SeekType {
    /** The offset is set to offset bytes. */
    kDirectIoSeekSet = 0,
    /** The offset is set to its current location plus offset bytes. */
    kDirectIoSeekCur,
    /** The offset is set to the size of the file plus offset bytes. */
    kDirectIoSeekEnd,
  };

  /**
   * @brief Constructs this object without opening it yet.
   * @param[in] path Path of the file to manipulate
   * @param[in] emulation Optional argument to emulate slower devices.
   */
  DirectIoFile(
    const Path &path,
    const DeviceEmulationOptions &emulation = DeviceEmulationOptions());

  /** Automatically closes the file if it is opened. */
  ~DirectIoFile();

  // Disable default constructors
  DirectIoFile() CXX11_FUNC_DELETE;
  DirectIoFile(const DirectIoFile &) CXX11_FUNC_DELETE;
  DirectIoFile& operator=(const DirectIoFile &) CXX11_FUNC_DELETE;

  /**
   * @brief Tries to open the file for the specified volume.
   * @param[in] read whether to allow read accesses on the opened file
   * @param[in] write whether to allow write accesses on the opened file
   * @param[in] append whether to set initial offset at the end of the file
   * @param[in] create whether to create the file. if already exists, does nothing.
   */
  ErrorStack      open(bool read, bool write, bool append, bool create);

  /** Whether the file is already and successfully opened.*/
  bool            is_opened() const { return descriptor_ != kInvalidDescriptor; }

  /**
   * @brief Close the file if not yet closed.
   * @return Whether successfully closed. Even it fails, the file descriptor is already invalid.
   * You cannot call this method again to close it appropriately.
   */
  bool            close();

  /**
   * @brief Sequentially read the given amount of contents from the current position.
   * @param[in] desired_bytes Number of bytes to read. If we can't read this many bytes,
   * we return errors.
   * @param[out] buffer Memory to copy into. As this is Direct-IO, it must be aligned.
   * @pre is_opened()
   * @pre buffer.get_size() >= desired_bytes
   * @pre (buffer->get_alignment() & 0xFFF) == 0 (4kb alignment)
   */
  ErrorStack      read(uint64_t desired_bytes, foedus::memory::AlignedMemory* buffer);
  /** Memory slice version. */
  ErrorStack      read(uint64_t desired_bytes, const foedus::memory::AlignedMemorySlice& slice);
  /** A version that receives a raw pointer that has to be aligned (be careful to use this ver). */
  ErrorStack      read_raw(uint64_t desired_bytes, void* buffer);

  /**
   * @brief Sequentially write the given amount of contents from the current position.
   * @param[in] desired_bytes Number of bytes to write. If we can't write this many bytes,
   * we return errors.
   * @param[in] buffer Memory to read from. As this is Direct-IO, it must be aligned.
   * @pre is_opened()
   * @pre buffer.get_size() >= desired_bytes
   * @pre (buffer->get_alignment() & 0xFFF) == 0 (4kb alignment)
   */
  ErrorStack      write(uint64_t desired_bytes, const foedus::memory::AlignedMemory& buffer);
  /** Memory slice version. */
  ErrorStack      write(uint64_t desired_bytes, const foedus::memory::AlignedMemorySlice& slice);
  /** A version that receives a raw pointer that has to be aligned (be careful to use this ver). */
  ErrorStack      write_raw(uint64_t desired_bytes, const void* buffer);

  /**
   * @brief Discard the content of the file after the given offset.
   * @param[in] new_length The size of this file would become this value.
   * @param[in] sync Whether to call fsync on the file and its parent folder after the truncation.
   * @details
   * After a successful call of this method, the cursor would be at new_length, too.
   * This method is used only when we restart the engine in order to discard non-durable
   * parts of log files (regions after durable watermark).
   * @pre is_opened()
   */
  ErrorStack      truncate(uint64_t new_length, bool sync = false);

  /**
   * Sets the position of the next byte to be written/extracted from/to the stream.
   * @pre is_opened()
   */
  ErrorStack      seek(uint64_t offset, SeekType seek_type);


  /**
   * @brief Analogues of POSIX fsync().
   * @details
   * @par POSIX fsync()
   * Transfers ("flushes") all modified in-core data of (i.e., modified buffer cache pages for)
   * the file referred to by the file descriptor fd to the disk device (or other permanent
   * storage device) so that all changed information can be retrieved even after the system
   * crashed or was rebooted.
   * @par No fdatasync analogue
   * All of our data writes are appends. So, there is no case we are benefited by fdatasync.
   * Hence, we have only fsync() analogue.
   * @pre is_opened()
   * @pre is_write()
   */
  ErrorStack      sync();


  Path                    get_path() const { return path_; }
  DeviceEmulationOptions  get_emulation() const { return emulation_; }
  file_descriptor         get_descriptor() const { return descriptor_; }
  uint64_t                get_current_offset() const { return current_offset_; }
  bool                    is_read() const { return read_; }
  bool                    is_write() const { return write_; }

  std::string             to_string() const;
  friend std::ostream&    operator<<(std::ostream& o, const DirectIoFile& v);

 private:
  /** The path of the file being manipulated. */
  Path                    path_;

  /** Settings to emulate slower device. */
  DeviceEmulationOptions  emulation_;

  /** File descriptor of the file. */
  file_descriptor         descriptor_;

  /** whether to allow read accesses on the opened file. */
  bool                    read_;

  /** whether to allow write accesses on the opened file. */
  bool                    write_;

  /** Current byte position of this stream. */
  uint64_t                current_offset_;
};
}  // namespace fs
}  // namespace foedus
#endif  // FOEDUS_FS_DIRECT_IO_FILE_HPP_

