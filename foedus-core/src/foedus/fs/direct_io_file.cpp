/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/fs/device_emulation_options.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <fcntl.h>
#include <glog/logging.h>
#include <foedus/assert_nd.hpp>
#include <ostream>
#include <sstream>
#include <string>
namespace foedus {
namespace fs {
const uint64_t ODIRECT_ALIGNMENT = 0x1000;
inline bool is_odirect_aligned(uint64_t value) {
    return (value % ODIRECT_ALIGNMENT) == 0;
}
inline bool is_odirect_aligned(void* ptr) {
    return (reinterpret_cast<uintptr_t>(ptr) % ODIRECT_ALIGNMENT) == 0;
}

DirectIoFile::DirectIoFile(
    const Path &path,
    const DeviceEmulationOptions &emulation)
    : path_(path), emulation_(emulation),
    descriptor_(INVALID_DESCRIPTOR), read_(false), write_(false), current_offset_(0) {
}

DirectIoFile::~DirectIoFile() {
    close();
}

ErrorStack DirectIoFile::open(bool read, bool write, bool append, bool create) {
    if (descriptor_ != INVALID_DESCRIPTOR) {
        return ERROR_STACK_MSG(ERROR_CODE_FS_ALREADY_OPENED, path_.c_str());
    }
    Path folder(path_.parent_path());
    if (!exists(folder)) {
        if (!create_directories(folder, true)) {
            LOG(ERROR) << "DirectIoFile::open(): failed to create parent folder: "
                << folder << ". err=" << assorted::os_error();
            return ERROR_STACK_MSG(ERROR_CODE_FS_MKDIR_FAILED, folder.c_str());
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
    // In that case, let's
    if (descriptor_ == INVALID_DESCRIPTOR && (oflags & O_DIRECT) == O_DIRECT && errno == EINVAL) {
        descriptor_ = ::open(path_.c_str(), oflags ^ O_DIRECT, permissions);
        if (descriptor_ != INVALID_DESCRIPTOR) {
            // Okay, O_DIRECT was the cause. Just complain. go on.
            LOG(WARNING) << "DirectIoFile::open(): O_DIRECT flag for " << path_
                << " was rejected and automatically removed. This usually means you specified"
                << " tmpfs, such as /tmp, /dev/shm. Such non-durable devices should be used only"
                << " for testing and performance experiments."
                << " Related URL: http://www.gossamer-threads.com/lists/linux/kernel/720702";
        }
        // else the normal error flow below.
    }

    if (descriptor_ == INVALID_DESCRIPTOR) {
        LOG(ERROR) << "DirectIoFile::open(): failed to open: " << path_
            << ". err=" << assorted::os_error();
        return ERROR_STACK_MSG(ERROR_CODE_FS_FAILED_TO_OPEN, path_.c_str());
    } else {
        read_ = read;
        write_ = write;
        current_offset_ = 0;
        if (append) {
            current_offset_ = file_size(path_);
        }
        LOG(INFO) << "DirectIoFile::open(): successfully opened. " << *this;
        return RET_OK;
    }
}

bool DirectIoFile::close() {
    if (descriptor_ != INVALID_DESCRIPTOR) {
        int ret = ::close(descriptor_);
        LOG(INFO) << "DirectIoFile::close(): closed. " << *this;
        if (ret != 0) {
            // Error at file close is nasty, we can't do much. We just report it in log.
            LOG(ERROR) << "DirectIoFile::close(): error:" << foedus::assorted::os_error()
                << " file=" << *this << ".";
            ASSERT_ND(false);  // crash only in debug mode
        }
        descriptor_ = INVALID_DESCRIPTOR;
        return ret == 0;
    }
    return false;
}
ErrorStack DirectIoFile::read(uint64_t desired_bytes, memory::AlignedMemory* buffer) {
    return read(desired_bytes, memory::AlignedMemorySlice(buffer));
}
ErrorStack DirectIoFile::read(uint64_t desired_bytes, memory::AlignedMemorySlice buffer) {
    if (!is_opened()) {
        return ERROR_STACK_MSG(ERROR_CODE_FS_NOT_OPENED, to_string().c_str());
    }
    if (desired_bytes == 0) {
        return RET_OK;
    }
    if (desired_bytes > buffer.count_) {
        LOG(ERROR) << "DirectIoFile::read(): too small buffer is given. desired_bytes="
            << desired_bytes << ", buffer=" << buffer;
        return ERROR_STACK_MSG(ERROR_CODE_FS_BUFFER_TOO_SMALL, to_string().c_str());
    }
    if (!is_odirect_aligned(buffer.memory_->get_alignment())
            || !is_odirect_aligned(buffer.get_block())
            || !is_odirect_aligned(desired_bytes)) {
        LOG(ERROR) << "DirectIoFile::read(): non-aligned input is given. buffer=" << buffer
            << ", desired_bytes=" << desired_bytes;
        return ERROR_STACK_MSG(ERROR_CODE_FS_BUFFER_NOT_ALIGNED, to_string().c_str());
    }

    // underlying POSIX filesystem might split the read for severel reasons. so, while loop.
    uint64_t total_read = 0;
    uint64_t remaining = desired_bytes;
    while (remaining > 0) {
        char* position = reinterpret_cast<char*>(buffer.get_block()) + total_read;
        ASSERT_ND(is_odirect_aligned(position));
        ssize_t read_bytes = ::read(descriptor_, position, remaining);
        if (read_bytes <= 0) {
            // zero means end of file (unexpected). negative value means error.
            LOG(ERROR) << "DirectIoFile::read(): error. this=" << *this << " buffer=" << buffer
                << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", read_bytes=" << read_bytes
                << ", err=" << assorted::os_error();
            return ERROR_STACK_MSG(ERROR_CODE_FS_TOO_SHORT_READ, to_string().c_str());
        }

        if (static_cast<uint64_t>(read_bytes) > remaining) {
            LOG(ERROR) << "DirectIoFile::read(): wtf? this=" << *this << " buffer=" << buffer
                << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", read_bytes=" << read_bytes
                << ", err=" << assorted::os_error();
            return ERROR_STACK_MSG(ERROR_CODE_FS_EXCESS_READ, to_string().c_str());
        } else if (!emulation_.disable_direct_io_ && !is_odirect_aligned(read_bytes)) {
            LOG(FATAL) << "DirectIoFile::read(): wtf2? this=" << *this << " buffer=" << buffer
                << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", read_bytes=" << read_bytes
                << ", err=" << assorted::os_error();
            return ERROR_STACK_MSG(ERROR_CODE_FS_RESULT_NOT_ALIGNED, to_string().c_str());
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
    return RET_OK;
}

ErrorStack DirectIoFile::write(uint64_t desired_bytes, const memory::AlignedMemory& buffer) {
    return write(desired_bytes, memory::AlignedMemorySlice(
        const_cast<memory::AlignedMemory*>(&buffer)));
}

ErrorStack DirectIoFile::write(uint64_t desired_bytes, memory::AlignedMemorySlice buffer) {
    if (!is_opened()) {
        return ERROR_STACK_MSG(ERROR_CODE_FS_NOT_OPENED, to_string().c_str());
    }
    if (desired_bytes == 0) {
        return RET_OK;
    }
    ASSERT_ND(buffer.is_valid());
    if (desired_bytes > buffer.count_) {
        LOG(ERROR) << "DirectIoFile::write(): too small buffer is given. desired_bytes="
            << desired_bytes << ", buffer=" << buffer;
        return ERROR_STACK_MSG(ERROR_CODE_FS_BUFFER_TOO_SMALL, to_string().c_str());
    }
    if (!is_odirect_aligned(buffer.memory_->get_alignment())
            || !is_odirect_aligned(buffer.get_block())
            || !is_odirect_aligned(desired_bytes)) {
        LOG(ERROR) << "DirectIoFile::write(): non-aligned input is given. buffer=" << buffer
            << ", desired_bytes=" << desired_bytes;
        return ERROR_STACK_MSG(ERROR_CODE_FS_BUFFER_NOT_ALIGNED, to_string().c_str());
    }

    // underlying POSIX filesystem might split the write for severel reasons. so, while loop.
    VLOG(1) << "DirectIoFile::write(). desired_bytes=" << desired_bytes << ", buffer=" << buffer;
    uint64_t total_written = 0;
    uint64_t remaining = desired_bytes;
    while (remaining > 0) {
        void* position = reinterpret_cast<char*>(buffer.get_block()) + total_written;
        VLOG(1) << "DirectIoFile::write(). position=" << position;
        ASSERT_ND(is_odirect_aligned(position));
        ssize_t written_bytes = ::write(descriptor_, position, remaining);
        if (written_bytes < 0) {
            // negative value means error.
            LOG(ERROR) << "DirectIoFile::write(): error. this=" << *this << " buffer=" << buffer
                << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", written_bytes=" << written_bytes
                << ", err=" << assorted::os_error();
            // TODO(Hideaki) more error codes depending on errno. but mostly it should be disk-full
            return ERROR_STACK_MSG(ERROR_CODE_FS_WRITE_FAIL, to_string().c_str());
        }

        if (static_cast<uint64_t>(written_bytes) > remaining) {
            LOG(ERROR) << "DirectIoFile::write(): wtf? this=" << *this << " buffer=" << buffer
                << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", written_bytes=" << written_bytes
                << ", err=" << assorted::os_error();
            return ERROR_STACK_MSG(ERROR_CODE_FS_EXCESS_WRITE, to_string().c_str());
        } else if (!emulation_.disable_direct_io_ && !is_odirect_aligned(written_bytes)) {
            LOG(FATAL) << "DirectIoFile::write(): wtf2? this=" << *this << " buffer=" << buffer
                << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", written_bytes=" << written_bytes
                << ", err=" << assorted::os_error();
            return ERROR_STACK_MSG(ERROR_CODE_FS_RESULT_NOT_ALIGNED, to_string().c_str());
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
    return RET_OK;
}

ErrorStack DirectIoFile::truncate(uint64_t new_length, bool sync) {
    if (!is_odirect_aligned(new_length)) {
        LOG(ERROR) << "DirectIoFile::truncate(): non-aligned input is given. "
            << " new_length=" << new_length;
        return ERROR_STACK(ERROR_CODE_FS_BUFFER_NOT_ALIGNED);
    }
    LOG(INFO) << "DirectIoFile::truncate(): truncating " << *this << " to " << new_length
        << " bytes..";
    if (!is_opened()) {
        return ERROR_STACK_MSG(ERROR_CODE_FS_NOT_OPENED, to_string().c_str());
    }

    if (::ftruncate(descriptor_, new_length) != 0) {
        LOG(ERROR) << "DirectIoFile::truncate(): failed. this=" << *this
            << " err=" << assorted::os_error();
        return ERROR_STACK_MSG(ERROR_CODE_FS_TRUNCATE_FAILED, to_string().c_str());
    }
    current_offset_ = new_length;
    if (sync) {
        LOG(INFO) << "DirectIoFile::truncate(): also fsync..";
        foedus::fs::fsync(path_, true);
    }
    return RET_OK;
}

ErrorStack DirectIoFile::seek(uint64_t offset, SeekType seek_type) {
    if (!is_odirect_aligned(offset)) {
        LOG(ERROR) << "DirectIoFile::seek(): non-aligned input is given. offset=" << offset;
        return ERROR_STACK(ERROR_CODE_FS_BUFFER_NOT_ALIGNED);
    }
    __off_t ret;
    switch (seek_type) {
        case DIRECT_IO_SEEK_SET:
            ret = ::lseek(descriptor_, offset, SEEK_SET);
            break;
        case DIRECT_IO_SEEK_CUR:
            ret = ::lseek(descriptor_, offset, SEEK_CUR);
            break;
        case DIRECT_IO_SEEK_END:
            ret = ::lseek(descriptor_, offset, SEEK_END);
            break;
        default:
            LOG(ERROR) << "DirectIoFile::seek(): wtf?? seek_type=" << seek_type;
            return ERROR_STACK_MSG(ERROR_CODE_FS_BAD_SEEK_INPUT, to_string().c_str());
    }
    if (ret < 0) {
        LOG(ERROR) << "DirectIoFile::seek(): failed. err=" << assorted::os_error();
        return ERROR_STACK_MSG(ERROR_CODE_FS_SEEK_FAILED, to_string().c_str());
    }
    current_offset_ = ret;
    return RET_OK;
}

ErrorStack DirectIoFile::sync() {
    if (!is_opened()) {
        return ERROR_STACK(ERROR_CODE_FS_NOT_OPENED);
    }
    if (!is_write()) {
        return ERROR_STACK(ERROR_CODE_INVALID_PARAMETER);
    }

    int ret = ::fsync(descriptor_);
    if (ret != 0) {
        LOG(ERROR) << "DirectIoFile::sync(): fsync failed. this=" << *this
            << ", err=" << assorted::os_error();
        return ERROR_STACK_MSG(ERROR_CODE_FS_SYNC_FAILED, to_string().c_str());
    }

    return RET_OK;
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
