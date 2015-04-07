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
#include <dirent.h>

#include <ostream>
#include <string>
#include <vector>

#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"

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

