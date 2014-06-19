/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <dirent.h>
#include <ostream>
#include <string>
#include <vector>
namespace foedus {
namespace fs {
Path::Path(const std::string& s) {
  if (s.size() > 0 && s.at(0) == '~'
    && (s.size() == 1 || s.at(1) == '/')) {
    // starts with ~/ or ~ alone: resolve it as home folder
    pathname_ = home_path().pathname_;
    pathname_.append(s.substr(1));
  } else {
    if (s.empty() || s.at(0) == kPreferredSeparator) {
      pathname_ = s;
    } else {
      Path tmp = current_path();
      tmp /= s;
      pathname_ = tmp.string();
    }
  }
}

Path Path::filename() const {
  size_t pos = pathname_.find_last_of(kPreferredSeparator);
  if (pos == pathname_.size()) {
    return Path(pathname_);
  } else {
    return Path(pathname_.c_str() + pos);
  }
}

Path Path::parent_path() const {
  size_t pos = pathname_.find_last_of(kPreferredSeparator);
  if (pos == pathname_.size()) {
    return Path();
  } else {
    return Path(pathname_.substr(0, pos));
  }
}

std::vector< Path > Path::child_paths() const {
  std::vector< Path > children;
  if (is_directory(*this)) {
    DIR *d = ::opendir(c_str());
    if (d) {
      for (dirent *e = ::readdir(d); e != nullptr; e = ::readdir(d)) {
        if (e->d_name == std::string(".") || e->d_name == std::string("..")) {
          continue;
        }
        Path child(*this);
        child /= std::string(e->d_name);
        children.emplace_back(child);
      }
      ::closedir(d);
    }
  }
  return children;
}


std::ostream& operator<<(std::ostream& o, const Path& v) {
  o << v.string();
  return o;
}

}  // namespace fs
}  // namespace foedus

