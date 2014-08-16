/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/fs/direct_io_file.hpp"

#include <fcntl.h>
#include <glog/logging.h>

#include <ostream>
#include <sstream>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/rdtsc.hpp"
#include "foedus/fs/device_emulation_options.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"

namespace foedus {
namespace fs {
const uint64_t kOdirectAlignment = 0x1000;
inline bool is_odirect_aligned(uint64_t value) {
  return (value % kOdirectAlignment) == 0;
}
inline bool is_odirect_aligned(const void* ptr) {
  return (reinterpret_cast<uintptr_t>(ptr) % kOdirectAlignment) == 0;
}

DirectIoFile::DirectIoFile(
  const Path &path,
  const DeviceEmulationOptions &emulation)
  : path_(path), emulation_(emulation),
  descriptor_(kInvalidDescriptor), read_(false), write_(false), current_offset_(0) {
}

DirectIoFile::~DirectIoFile() {
  close();
}

ErrorCode  DirectIoFile::open(bool read, bool write, bool append, bool create) {
  if (descriptor_ != kInvalidDescriptor) {
      LOG(ERROR) << "DirectIoFile::open(): already opened. this=" << *this;
    return kErrorCodeFsAlreadyOpened;
  }
  Path folder(path_.parent_path());
  if (!exists(folder)) {
    if (!create_directories(folder, true)) {
      LOG(ERROR) << "DirectIoFile::open(): failed to create parent folder: "
        << folder << ". err=" << assorted::os_error();
      return kErrorCodeFsMkdirFailed;
    }
  }

  LOG(INFO) << "DirectIoFile::open(): opening: " << path_ << "..  read =" << read << " write="
    << write << ", append=" << append << ", create=" << create;
  int oflags = O_LARGEFILE;
  if (!emulation_.disable_direct_io_) {
    oflags |= O_DIRECT;
  }
  if (read) {
    if (write) {
      oflags |= O_RDWR;
    } else {
      oflags |= O_RDONLY;
    }
  } else if (write) {
    // oflags |= O_WRONLY;
    oflags |= O_RDWR;
  }
  if (append) {
    oflags |= O_APPEND;
  }
  if (create) {
    oflags |= O_CREAT;
  }
  mode_t permissions = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
  descriptor_ = ::open(path_.c_str(), oflags, permissions);

  // tmpfs (such as /tmp, /dev/shm) refuses to receive O_DIRECT, returning EINVAL (22).
  // In that case, let's retry without O_DIRECT flag. MySQL does similar thing, too.
  if (descriptor_ == kInvalidDescriptor && (oflags & O_DIRECT) == O_DIRECT && errno == EINVAL) {
    descriptor_ = ::open(path_.c_str(), oflags ^ O_DIRECT, permissions);
    if (descriptor_ != kInvalidDescriptor) {
      // Okay, O_DIRECT was the cause. Just complain. go on.
      LOG(WARNING) << "DirectIoFile::open(): O_DIRECT flag for " << path_
        << " was rejected and automatically removed. This usually means you specified"
        << " tmpfs, such as /tmp, /dev/shm. Such non-durable devices should be used only"
        << " for testing and performance experiments."
        << " Related URL: http://www.gossamer-threads.com/lists/linux/kernel/720702";
    }
    // else the normal error flow below.
  }

  if (descriptor_ == kInvalidDescriptor) {
    LOG(ERROR) << "DirectIoFile::open(): failed to open: " << path_
      << ". err=" << assorted::os_error();
    return kErrorCodeFsFailedToOpen;
  } else {
    read_ = read;
    write_ = write;
    current_offset_ = 0;
    if (append) {
      current_offset_ = file_size(path_);
    }
    LOG(INFO) << "DirectIoFile::open(): successfully opened. " << *this;
    return kErrorCodeOk;
  }
}

bool DirectIoFile::close() {
  if (descriptor_ != kInvalidDescriptor) {
    int ret = ::close(descriptor_);
    LOG(INFO) << "DirectIoFile::close(): closed. " << *this;
    if (ret != 0) {
      // Error at file close is nasty, we can't do much. We just report it in log.
      LOG(ERROR) << "DirectIoFile::close(): error:" << foedus::assorted::os_error()
        << " file=" << *this << ".";
      ASSERT_ND(false);  // crash only in debug mode
    }
    descriptor_ = kInvalidDescriptor;
    return ret == 0;
  }
  return false;
}
ErrorCode  DirectIoFile::read(uint64_t desired_bytes, memory::AlignedMemory* buffer) {
  return read(desired_bytes, memory::AlignedMemorySlice(buffer));
}
ErrorCode  DirectIoFile::read(uint64_t desired_bytes, const memory::AlignedMemorySlice& buffer) {
  ASSERT_ND(!emulation_.null_device_);
  if (desired_bytes > buffer.count_) {
    LOG(ERROR) << "DirectIoFile::read(): too small buffer is given. desired_bytes="
      << desired_bytes << ", buffer=" << buffer;
    return kErrorCodeFsBufferTooSmall;
  } else if (!is_odirect_aligned(buffer.memory_->get_alignment())
      || !is_odirect_aligned(buffer.get_block())
      || !is_odirect_aligned(desired_bytes)) {
    LOG(ERROR) << "DirectIoFile::read(): non-aligned input is given. buffer=" << buffer
      << ", desired_bytes=" << desired_bytes;
    return kErrorCodeFsBufferNotAligned;
  }
  return read_raw(desired_bytes, buffer.get_block());
}
ErrorCode  DirectIoFile::read_raw(uint64_t desired_bytes, void* buffer) {
  ASSERT_ND(!emulation_.null_device_);
  if (!is_opened()) {
    LOG(ERROR) << "File not opened yet, or closed. this=" << *this;
    return kErrorCodeFsNotOpened;
  } else if (desired_bytes == 0) {
    return kErrorCodeOk;
  }

  // underlying POSIX filesystem might split the read for severel reasons. so, while loop.
  uint64_t total_read = 0;
  uint64_t remaining = desired_bytes;
  while (remaining > 0) {
    char* position = reinterpret_cast<char*>(buffer) + total_read;
    ASSERT_ND(is_odirect_aligned(position));
    ssize_t read_bytes = ::read(descriptor_, position, remaining);
    if (read_bytes <= 0) {
      // zero means end of file (unexpected). negative value means error.
      LOG(ERROR) << "DirectIoFile::read(): error. this=" << *this
        << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining << ", read_bytes=" << read_bytes
        << ", err=" << assorted::os_error();
      return kErrorCodeFsTooShortRead;
    }

    if (static_cast<uint64_t>(read_bytes) > remaining) {
      LOG(ERROR) << "DirectIoFile::read(): wtf? this=" << *this
        << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining << ", read_bytes=" << read_bytes
        << ", err=" << assorted::os_error();
      return kErrorCodeFsExcessRead;
    } else if (!emulation_.disable_direct_io_ && !is_odirect_aligned(read_bytes)) {
      LOG(FATAL) << "DirectIoFile::read(): wtf2? this=" << *this
        << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining << ", read_bytes=" << read_bytes
        << ", err=" << assorted::os_error();
      return kErrorCodeFsResultNotAligned;
    }

    total_read += read_bytes;
    remaining -= read_bytes;
    current_offset_ += read_bytes;
    if (remaining > 0) {
      LOG(INFO) << "Interesting. POSIX read() didn't complete the reads in one call."
        << " total_read=" << total_read << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining;
    }
  }
  if (emulation_.emulated_read_kb_cycles_ > 0) {
    debugging::wait_rdtsc_cycles(emulation_.emulated_read_kb_cycles_ * (desired_bytes >> 10));
  }
  return kErrorCodeOk;
}

ErrorCode  DirectIoFile::write(uint64_t desired_bytes, const memory::AlignedMemory& buffer) {
  return write(desired_bytes, memory::AlignedMemorySlice(
    const_cast<memory::AlignedMemory*>(&buffer)));
}

ErrorCode  DirectIoFile::write(uint64_t desired_bytes, const memory::AlignedMemorySlice& buffer) {
  ASSERT_ND(buffer.is_valid());
  if (desired_bytes > buffer.count_) {
    LOG(ERROR) << "DirectIoFile::write(): too small buffer is given. desired_bytes="
      << desired_bytes << ", buffer=" << buffer;
    return kErrorCodeFsBufferTooSmall;
  } else if (!is_odirect_aligned(buffer.memory_->get_alignment())
      || !is_odirect_aligned(buffer.get_block())
      || !is_odirect_aligned(desired_bytes)) {
    LOG(ERROR) << "DirectIoFile::write(): non-aligned input is given. buffer=" << buffer
      << ", desired_bytes=" << desired_bytes;
    return kErrorCodeFsBufferNotAligned;
  }
  return write_raw(desired_bytes, buffer.get_block());
}
ErrorCode  DirectIoFile::write_raw(uint64_t desired_bytes, const void* buffer) {
  if (!is_opened()) {
    LOG(ERROR) << "File not opened yet, or closed. this=" << *this;
    return kErrorCodeFsNotOpened;
  } else if (desired_bytes == 0) {
    return kErrorCodeOk;
  }

  if (emulation_.null_device_) {
    return kErrorCodeOk;
  }

  // underlying POSIX filesystem might split the write for severel reasons. so, while loop.
  VLOG(1) << "DirectIoFile::write(). desired_bytes=" << desired_bytes << ", buffer=" << buffer;
  uint64_t total_written = 0;
  uint64_t remaining = desired_bytes;
  while (remaining > 0) {
    const void* position = reinterpret_cast<const char*>(buffer) + total_written;
    VLOG(1) << "DirectIoFile::write(). position=" << position;
    ASSERT_ND(is_odirect_aligned(position));
    ssize_t written_bytes = ::write(descriptor_, position, remaining);
    if (written_bytes < 0) {
      // negative value means error.
      LOG(ERROR) << "DirectIoFile::write(): error. this=" << *this
        << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining << ", written_bytes=" << written_bytes
        << ", err=" << assorted::os_error();
      // TODO(Hideaki) more error codes depending on errno. but mostly it should be disk-full
      return kErrorCodeFsWriteFail;
    }

    if (static_cast<uint64_t>(written_bytes) > remaining) {
      LOG(ERROR) << "DirectIoFile::write(): wtf? this=" << *this
        << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining << ", written_bytes=" << written_bytes
        << ", err=" << assorted::os_error();
      return kErrorCodeFsExcessWrite;
    } else if (!emulation_.disable_direct_io_ && !is_odirect_aligned(written_bytes)) {
      LOG(FATAL) << "DirectIoFile::write(): wtf2? this=" << *this
        << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining << ", written_bytes=" << written_bytes
        << ", err=" << assorted::os_error();
      return kErrorCodeFsResultNotAligned;
    }

    total_written += written_bytes;
    remaining -= written_bytes;
    current_offset_ += written_bytes;
    if (remaining > 0) {
      LOG(INFO) << "Interesting. POSIX write() didn't complete the writes in one call."
        << " total_written=" << total_written << ", desired_bytes=" << desired_bytes
        << ", remaining=" << remaining;
    }
  }
  if (emulation_.emulated_write_kb_cycles_ > 0) {
    debugging::wait_rdtsc_cycles(emulation_.emulated_write_kb_cycles_ * (desired_bytes >> 10));
  }
  return kErrorCodeOk;
}

ErrorCode  DirectIoFile::truncate(uint64_t new_length, bool sync) {
  if (!is_odirect_aligned(new_length)) {
    LOG(ERROR) << "DirectIoFile::truncate(): non-aligned input is given. "
      << " new_length=" << new_length;
    return kErrorCodeFsBufferNotAligned;
  }
  LOG(INFO) << "DirectIoFile::truncate(): truncating " << *this << " to " << new_length
    << " bytes..";
  if (!is_opened()) {
    return kErrorCodeFsNotOpened;
  }

  if (emulation_.null_device_) {
    current_offset_ = new_length;
    return kErrorCodeOk;
  }

  if (::ftruncate(descriptor_, new_length) != 0) {
    LOG(ERROR) << "DirectIoFile::truncate(): failed. this=" << *this
      << " err=" << assorted::os_error();
    return kErrorCodeFsTruncateFailed;
  }
  current_offset_ = new_length;
  if (sync) {
    LOG(INFO) << "DirectIoFile::truncate(): also fsync..";
    foedus::fs::fsync(path_, true);
  }
  return kErrorCodeOk;
}

ErrorCode  DirectIoFile::seek(uint64_t offset, SeekType seek_type) {
  if (!is_odirect_aligned(offset)) {
    LOG(ERROR) << "DirectIoFile::seek(): non-aligned input is given. offset=" << offset;
    return kErrorCodeFsBufferNotAligned;
  }
  if (emulation_.null_device_) {
    return kErrorCodeOk;
  }
  __off_t ret;
  switch (seek_type) {
    case kDirectIoSeekSet:
      ret = ::lseek(descriptor_, offset, SEEK_SET);
      break;
    case kDirectIoSeekCur:
      ret = ::lseek(descriptor_, offset, SEEK_CUR);
      break;
    case kDirectIoSeekEnd:
      ret = ::lseek(descriptor_, offset, SEEK_END);
      break;
    default:
      LOG(ERROR) << "DirectIoFile::seek(): wtf?? seek_type=" << seek_type;
      return kErrorCodeFsBadSeekInput;
  }
  if (ret < 0) {
    LOG(ERROR) << "DirectIoFile::seek(): failed. this=" << *this << ",err=" << assorted::os_error();
    return kErrorCodeFsSeekFailed;
  }
  current_offset_ = ret;
  if (emulation_.emulated_seek_latency_cycles_ > 0) {
    debugging::wait_rdtsc_cycles(emulation_.emulated_seek_latency_cycles_);
  }
  return kErrorCodeOk;
}

ErrorCode  DirectIoFile::sync() {
  if (!is_opened()) {
    LOG(ERROR) << "File not opened yet, or closed. this=" << *this;
    return kErrorCodeFsNotOpened;
  }
  if (!is_write()) {
    return kErrorCodeInvalidParameter;
  }

  if (emulation_.null_device_) {
    return kErrorCodeOk;
  }

  int ret = ::fsync(descriptor_);
  if (ret != 0) {
    LOG(ERROR) << "DirectIoFile::sync(): fsync failed. this=" << *this
      << ", err=" << assorted::os_error();
    return kErrorCodeFsSyncFailed;
  }

  return kErrorCodeOk;
}

std::string DirectIoFile::to_string() const {
  std::stringstream stream;
  stream << *this;
  return stream.str();
}
std::ostream& operator<<(std::ostream& o, const DirectIoFile& v) {
  o << "<DirectIoFile>"
    << "<path>" << v.get_path() << "</path>"
    << "<descriptor>" << v.get_descriptor() << "</descriptor>"
    << "<read>" << v.is_read() << "</read>"
    << "<write>" << v.is_write() << "</write>"
    << "<current_offset>" << v.get_current_offset() << "</current_offset>"
    << "</DirectIoFile>";
  return o;
}

}  // namespace fs
}  // namespace foedus
