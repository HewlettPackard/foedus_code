/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include "foedus/fs/filesystem.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/vfs.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/fs/path.hpp"

namespace foedus {
namespace fs {
FileStatus status(const Path& p) {
  struct stat path_stat;
  int ret = ::stat(p.c_str(), &path_stat);
  if (ret != 0) {
    // This is quite normal as we use this to check if a file exists. No message for it.
    if (errno == ENOENT || errno == ENOTDIR) {
      return FileStatus(kFileNotFound);
    }
    return FileStatus(kStatusError);
  } else if (S_ISDIR(path_stat.st_mode)) {
    return FileStatus(kDirectoryFile);
  } else if (S_ISREG(path_stat.st_mode)) {
    return FileStatus(kRegularFile);
  }
  return FileStatus(kTypeUnknown);
}

Path absolute(const std::string& p) {
  return Path(p);
}

Path current_path() {
  Path cur;
  for (size_t path_max = 128;; path_max *=2) {  // loop 'til buffer large enough
    std::vector<char> buf(path_max);
    if (::getcwd(&buf[0], path_max) == 0) {
      ASSERT_ND(errno == ERANGE);
    } else {
      cur = std::string(&buf[0]);
      break;
    }
  }
  return cur;
}

Path home_path() {
  const char *home = ::getenv("HOME");
  if (home) {
    return Path(home);
  } else {
    return Path();
  }
}

bool create_directories(const Path& p, bool sync) {
  if (exists(p)) {
    return true;
  }
  if (create_directory(p, sync)) {
    return true;
  }
  // if failed, create parent then try again
  Path parent = p.parent_path();
  if (parent.empty()) {
    return false;
  }
  if (!create_directories(parent, sync) && !exists(parent)) {
    return false;
  }
  // now ancestors exist.
  return create_directory(p, sync);
}

bool create_directory(const Path& p, bool sync) {
  int ret = ::mkdir(p.c_str(), S_IRWXU);
  if (ret != 0) {
    return false;
  }
  if (sync) {
    return fsync(p, true);
  } else {
    return true;
  }
}

uint64_t file_size(const Path& p) {
  struct stat path_stat;
  int ret = ::stat(p.c_str(), &path_stat);
  if (ret != 0) {
    return static_cast<uint64_t>(-1);
  }
  if (!S_ISREG(path_stat.st_mode)) {
    return static_cast<uint64_t>(-1);
  }
  return static_cast<uint64_t>(path_stat.st_size);
}

bool remove(const Path& p) {
  FileStatus s = status(p);
  if (!s.exists()) {
    return 0;
  } else if (s.is_regular_file()) {
    return std::remove(p.c_str()) == 0;
  } else {
    int ret = ::rmdir(p.c_str());
    if (ret == 0) {
      return true;
    } else {
      return false;
    }
  }
}

uint64_t remove_all(const Path& p) {
  uint64_t count = 1;
  std::vector< Path > child_paths = p.child_paths();
  for (Path child : child_paths) {
    count += remove_all(child);
  }
  remove(p);
  return count;
}

SpaceInfo space(const Path& p) {
  struct statfs vfs;
  SpaceInfo info;
  int ret = ::statfs(p.c_str(), &vfs);
  if (ret == 0) {
    info.capacity_    = static_cast<uint64_t>(vfs.f_blocks) * vfs.f_bsize;
    info.free_        = static_cast<uint64_t>(vfs.f_bfree)  * vfs.f_bsize;
    info.available_   = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_bsize;
  } else {
    info.available_ = 0;
    info.capacity_ = 0;
    info.free_ = 0;
  }
  return info;
}

std::string unique_name(uint64_t differentiator) {
  return unique_name("%%%%-%%%%-%%%%-%%%%", differentiator);
}
std::string unique_name(const std::string& model, uint64_t differentiator) {
  const char* kHexChars = "0123456789abcdef";
  uint64_t seed64 = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  seed64 += ::getpid();  // further use process ID to randomize. may help.
  seed64 ^= differentiator;
  uint32_t seed32 = (seed64 >> 32) ^ seed64;
  std::string s(model);
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '%') {                 // digit request
      seed32 = ::rand_r(&seed32);
      s[i] = kHexChars[seed32 & 0xf];  // convert to hex digit and replace
    }
  }
  return s;
}

std::ostream& operator<<(std::ostream& o, const SpaceInfo& v) {
  o << "SpaceInfo: available_=" << v.available_ << ", capacity_=" << v.capacity_
    << ", free_=" << v.free_;
  return o;
}

bool atomic_rename(const Path& old_path, const Path& new_path) {
  return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool fsync(const Path& path, bool sync_parent_directory) {
  int sync_ret;
  if (!is_directory(path)) {
    int descriptor = ::open(path.c_str(), O_RDONLY);
    if (descriptor < 0) {
      return false;
    }
    sync_ret = ::fsync(descriptor);
    ::close(descriptor);
  } else {
    DIR *dir = ::opendir(path.c_str());
    if (!dir) {
      return false;
    }
    int descriptor = ::dirfd(dir);
    if (descriptor < 0) {
      return false;
    }

    sync_ret = ::fsync(descriptor);
    ::closedir(dir);
  }

  if (sync_ret != 0) {
    return false;
  }

  if (sync_parent_directory) {
    return fsync(path.parent_path(), false);
  } else {
    return true;
  }
}
bool durable_atomic_rename(const Path& old_path, const Path& new_path) {
  if (!fsync(old_path, false)) {
    return false;
  }
  if (!atomic_rename(old_path, new_path)) {
    return false;
  }
  return fsync(new_path.parent_path(), false);
}

}  // namespace fs
}  // namespace foedus
