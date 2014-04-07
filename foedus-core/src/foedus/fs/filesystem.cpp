/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <glog/logging.h>

#include <dirent.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>
namespace foedus {
namespace fs {
Filesystem::Filesystem(const FilesystemOptions &options) {
    options_ = options;
}

FileStatus Filesystem::status(const Path& p) const {
    struct stat path_stat;
    int ret = ::stat(p.c_str(), &path_stat);
    if (ret != 0) {
        LOG(INFO) << "Filesystem::status(): stat() failed for " << p << ". errno=" << errno;
        if (errno == ENOENT || errno == ENOTDIR) {
            return FileStatus(file_not_found);
        }
        return FileStatus(status_error);
    } else if (S_ISDIR(path_stat.st_mode)) {
        return FileStatus(directory_file);
    } else if (S_ISREG(path_stat.st_mode)) {
        return FileStatus(regular_file);
    }
    return FileStatus(type_unknown);
}

Path Filesystem::absolute(const Path& p) const {
    if (p.empty() || p.string()[0] == Path::PREFERRED_SEPARATOR) {
        return p;
    }
    return current_path() /= p;
}

Path Filesystem::current_path() const {
    Path cur;
    for (size_t path_max = 128;; path_max *=2) {  // loop 'til buffer large enough
        std::vector<char> buf(path_max);
        if (::getcwd(&buf[0], path_max) == 0) {
            assert(errno == ERANGE);
        } else {
            cur = std::string(&buf[0]);
            break;
        }
    }
    return cur;
}

bool Filesystem::create_directories(const Path& p) const {
    int ret = ::mkdir(p.c_str(), S_IRWXU);
    if (ret == 0) {
        return true;
    }
    // if failed, create parent then try again
    Path parent = p.parent_path();
    if (parent.empty()) {
        return false;
    }
    if (!create_directories(parent)) {
        return false;
    }
    // now ancestors exist.
    return create_directory(p);
}

bool Filesystem::create_directory(const Path& p) const {
    int ret = ::mkdir(p.c_str(), S_IRWXU);
    if (ret != 0) {
        LOG(WARNING) << "Filesystem::create_directory() failed for " << p << ". errno=" << errno;
        return false;
    }
    return true;
}

uint64_t Filesystem::file_size(const Path& p) const {
    struct stat path_stat;
    int ret = ::stat(p.c_str(), &path_stat);
    if (ret != 0) {
        LOG(INFO) << "Filesystem::file_size(): stat() failed for " << p << ". errno=" << errno;
        return static_cast<uint64_t>(-1);
    }
    if (!S_ISREG(path_stat.st_mode)) {
        LOG(WARNING) << "Filesystem::file_size(): " << p << " is not a regular file!";
        return static_cast<uint64_t>(-1);
    }
    return static_cast<uint64_t>(path_stat.st_size);
}

bool Filesystem::remove(const Path& p) const {
    FileStatus s = status(p);
    if (!s.exists()) {
        VLOG(2) << "Filesystem::remove(): " << p << " doesn't exist";
        return 0;
    } else if (s.is_regular_file()) {
        return std::remove(p.c_str()) == 0;
    } else {
        int ret = ::rmdir(p.c_str());
        if (ret == 0) {
            return true;
        } else {
            LOG(WARNING) << "Filesystem::remove(): failed for " << p << ". errno=" << errno;
            return false;
        }
    }
}

uint64_t Filesystem::remove_all(const Path& p) const {
    uint64_t count = 1;
    FileStatus s = status(p);
    if (s.is_directory()) {
        DIR *d = ::opendir(p.c_str());
        if (d) {
            for (dirent *e = ::readdir(d); e != nullptr; e = ::readdir(d)) {
                if (e->d_name == std::string(".") || e->d_name == std::string("..")) {
                    continue;
                }
                Path next = p;
                next /= std::string(e->d_name);
                count += remove_all(next);
            }
            ::closedir(d);
        }
    }
     bool deleted = remove(p);
    if (!deleted) {
        LOG(WARNING) << "Filesystem::remove_all(): failed for " << p << ". deleted count=" << count;
    }
    return count;
}

SpaceInfo Filesystem::space(const Path& p) const {
    struct statfs vfs;
    SpaceInfo info;
    int ret = ::statfs(p.c_str(), &vfs);
    if (ret == 0) {
        info.capacity_    = static_cast<uint64_t>(vfs.f_blocks)* vfs.f_bsize;
        info.free_        = static_cast<uint64_t>(vfs.f_bfree)* vfs.f_bsize;
        info.available_   = static_cast<uint64_t>(vfs.f_bavail)* vfs.f_bsize;
        VLOG(1) << "Filesystem::space(): " << p << ": " << info;
    } else {
        LOG(WARNING) << "Filesystem::space(): failed for " << p << ". errno=" << errno;
        info.available_ = 0;
        info.capacity_ = 0;
        info.free_ = 0;
    }
    return info;
}

Path Filesystem::unique_path() const {
    return unique_path(Path("%%%%-%%%%-%%%%-%%%%"));
}
Path Filesystem::unique_path(const Path& model) const {
    const char* HEX_CHARS = "0123456789abcdef";
    unsigned int seed = static_cast<unsigned int>(std::time(nullptr));
    std::string s(model.string());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%') {                       // digit request
            seed = ::rand_r(&seed);
            s[i] = HEX_CHARS[seed & 0xf];  // convert to hex digit and replace
        }
    }
    return Path(s);
}

std::ostream& operator<<(std::ostream& o, const SpaceInfo& v) {
    o << "SpaceInfo: available_=" << v.available_ << ", capacity_=" << v.capacity_
        << ", free_=" << v.free_;
    return o;
}

}  // namespace fs
}  // namespace foedus
