/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/device_emulation_options.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <fcntl.h>
#include <glog/logging.h>
#include <cassert>
#include <ostream>
namespace foedus {
namespace fs {
DirectIoFile::DirectIoFile(
    const Path &path,
    const DeviceEmulationOptions &emulation)
    : path_(path), emulation_(emulation),
    descriptor_(INVALID_DESCRIPTOR), read_(false), write_(false), current_offset_(0) {
}

DirectIoFile::~DirectIoFile() {
    close();
}

ErrorCode DirectIoFile::open(bool read, bool write, bool append, bool create) {
    if (descriptor_ != INVALID_DESCRIPTOR) {
        LOG(WARNING) << "DirectIoFile::open(): already opened: " << path_;
        return ERROR_CODE_FS_ALREADY_OPENED;
    }

    LOG(INFO) << "DirectIoFile::open(): opening: " << path_ << "..  read =" << read << " write="
        << write << ", append=" << append << ", create=" << create;
    int oflags = O_NOATIME;
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
        oflags |= O_WRONLY;
    }
    if (append) {
        oflags |= O_APPEND;
    }
    if (create) {
        oflags |= O_CREAT;
    }
    descriptor_ = ::open(path_.c_str(), oflags);
    if (descriptor_ == INVALID_DESCRIPTOR) {
        LOG(ERROR) << "DirectIoFile::open(): failed to open: " << path_ << ". errno=" << errno;
        return ERROR_CODE_FS_FAILED_TO_OPEN;
    } else {
        read_ = read;
        write_ = write;
        current_offset_ = 0;
        if (append) {
            current_offset_ = file_size(path_);
        }
        LOG(INFO) << "DirectIoFile::open(): successfully opened. " << *this;
        return ERROR_CODE_OK;
    }
}

void DirectIoFile::close() {
    if (descriptor_ != INVALID_DESCRIPTOR) {
        int ret = ::close(descriptor_);
        LOG(INFO) << "DirectIoFile::close(): closed. " << *this << ". ret=" << ret;
        descriptor_ = INVALID_DESCRIPTOR;
    }
}

ErrorCode DirectIoFile::read(uint64_t desired_bytes, memory::AlignedMemory* buffer_ptr) {
    memory::AlignedMemory& buffer = *buffer_ptr;
    if (!is_opened()) {
        return ERROR_CODE_FS_NOT_OPENED;
    }
    if (desired_bytes == 0) {
        return ERROR_CODE_OK;
    }
    if (desired_bytes > buffer.get_size()) {
        LOG(ERROR) << "DirectIoFile::read(): too small buffer is given. desired_bytes="
            << desired_bytes << ", buffer=" << buffer;
        return ERROR_CODE_FS_BUFFER_TOO_SMALL;
    }
    if (!emulation_.disable_direct_io_ && (buffer.get_alignment() & 0xFFF) != 0) {
        LOG(ERROR) << "DirectIoFile::read(): non-aligned buffer is given. buffer=" << buffer;
        return ERROR_CODE_FS_BUFFER_NOT_ALIGNED;
    }

    // underlying POSIX filesystem might split the read for severel reasons. so, while loop.
    uint64_t total_read = 0;
    uint64_t remaining = desired_bytes;
    while (remaining > 0) {
        void* position = reinterpret_cast<char*>(buffer.get_block()) + total_read;
        ssize_t read_bytes = ::read(descriptor_, position, remaining);
        if (read_bytes <= 0 || errno != 0) {
            // zero means end of file (unexpected). negative value means error.
            LOG(ERROR) << "DirectIoFile::read(): error. this=" << *this << " buffer=" << buffer
                << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", read_bytes=" << read_bytes
                << ", errno=" << errno;
            return ERROR_CODE_FS_TOO_SHORT_READ;
        }

        if (static_cast<uint64_t>(read_bytes) > remaining) {
            LOG(ERROR) << "DirectIoFile::read(): wtf? this=" << *this << " buffer=" << buffer
                << ", total_read=" << total_read << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", read_bytes=" << read_bytes
                << ", errno=" << errno;
            return ERROR_CODE_FS_EXCESS_READ;
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
    return ERROR_CODE_OK;
}

ErrorCode DirectIoFile::write(uint64_t desired_bytes, const memory::AlignedMemory& buffer) {
    if (!is_opened()) {
        return ERROR_CODE_FS_NOT_OPENED;
    }
    if (desired_bytes == 0) {
        return ERROR_CODE_OK;
    }
    if (desired_bytes > buffer.get_size()) {
        LOG(ERROR) << "DirectIoFile::write(): too small buffer is given. desired_bytes="
            << desired_bytes << ", buffer=" << buffer;
        return ERROR_CODE_FS_BUFFER_TOO_SMALL;
    }
    if (!emulation_.disable_direct_io_ && (buffer.get_alignment() & 0xFFF) != 0) {
        LOG(ERROR) << "DirectIoFile::write(): non-aligned buffer is given. buffer=" << buffer;
        return ERROR_CODE_FS_BUFFER_NOT_ALIGNED;
    }

    // underlying POSIX filesystem might split the write for severel reasons. so, while loop.
    uint64_t total_written = 0;
    uint64_t remaining = desired_bytes;
    while (remaining > 0) {
        void* position = reinterpret_cast<char*>(buffer.get_block()) + total_written;
        ssize_t written_bytes = ::write(descriptor_, position, remaining);
        if (written_bytes < 0 || errno != 0) {
            // negative value means error.
            LOG(ERROR) << "DirectIoFile::write(): error. this=" << *this << " buffer=" << buffer
                << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", written_bytes=" << written_bytes
                << ", errno=" << errno;
            // TODO(Hideaki) more error codes depending on errno. but mostly it should be disk-full
            return ERROR_CODE_FS_WRITE_FAIL;
        }

        if (static_cast<uint64_t>(written_bytes) > remaining) {
            LOG(ERROR) << "DirectIoFile::write(): wtf? this=" << *this << " buffer=" << buffer
                << ", total_written=" << total_written << ", desired_bytes=" << desired_bytes
                << ", remaining=" << remaining << ", written_bytes=" << written_bytes
                << ", errno=" << errno;
            return ERROR_CODE_FS_EXCESS_WRITE;
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
    return ERROR_CODE_OK;
}


ErrorCode DirectIoFile::seek(uint64_t offset, SeekType seek_type) {
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
            return ERROR_CODE_FS_BAD_SEEK_INPUT;
    }
    if (ret < 0) {
        LOG(ERROR) << "DirectIoFile::seek(): failed. errno=" << errno;
        return ERROR_CODE_FS_SEEK_FAILED;
    }
    current_offset_ = ret;
    return ERROR_CODE_OK;
}

ErrorCode DirectIoFile::sync() {
    if (!is_opened()) {
        return ERROR_CODE_FS_NOT_OPENED;
    }
    if (!is_write()) {
        return ERROR_CODE_INVALID_PARAMETER;
    }

    int ret = ::fsync(descriptor_);
    if (ret != 0) {
        LOG(ERROR) << "DirectIoFile::sync(): fsync failed. this=" << *this << ", errno=" << errno;
        return ERROR_CODE_FS_SYNC_FAILED;
    }

    return ERROR_CODE_OK;
}

std::ostream& operator<<(std::ostream& o, const DirectIoFile& v) {
    o << "DirectIoFile" << std::endl;
    o << v.get_path();
    o << v.get_emulation();
    o << "  descriptor = " << v.get_descriptor() << std::endl;
    o << "  read = " << v.is_read() << ", write = " << v.is_write() << std::endl;
    o << "  current_offset = " << v.get_current_offset() << std::endl;
    return o;
}

}  // namespace fs
}  // namespace foedus
