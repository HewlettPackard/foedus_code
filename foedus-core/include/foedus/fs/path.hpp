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
#ifndef FOEDUS_FS_PATH_HPP_
#define FOEDUS_FS_PATH_HPP_
#include <iosfwd>
#include <string>
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"

namespace foedus {
namespace fs {
/**
 * @brief Analogue of boost::filesystem::path.
 * @ingroup FILESYSTEM
 * @details
 * Unlike boost::filesystem::path, this \b always brings a full path.
 * As soon as this object is instantiated, we convert it to an absolute path with absolute().
 * @todo Support Windows. MUCH later.
 */
class Path {
 public:
  static const char kPreferredSeparator = '/';

  Path() {}
  Path(const Path& p) : pathname_(p.pathname_) {}
  /** This one resolves ~ at beginning. */
  explicit Path(const std::string& s);

  Path& operator=(const Path& p) { pathname_ = p.pathname_; return *this; }
  Path& operator=(const std::string& s) { pathname_ = s; return *this; }
  Path& operator+=(const Path& p)         {pathname_ += p.pathname_; return *this;}
  Path& operator+=(const std::string& s)  {pathname_ += s; return *this;}

  void append_separator_if_needed() {
    if (!pathname_.empty() && pathname_.at(pathname_.size() - 1) != kPreferredSeparator) {
      pathname_ += kPreferredSeparator;
    }
  }
  Path& operator/=(const Path& p) { return operator/=(p.pathname_); }
  Path& operator/=(const std::string& s) {
    append_separator_if_needed();
    pathname_ += s;
    return *this;
  }

  const std::string&  native() const { return pathname_; }
  const char*         c_str()  const { return pathname_.c_str(); }
  const std::string&  string() const { return pathname_; }

  int compare(const Path& p) const CXX11_NOEXCEPT { return pathname_.compare(p.pathname_); }
  int compare(const std::string& s) const { return compare(Path(s)); }

  Path    parent_path() const;
  std::vector< Path > child_paths() const;
  Path    filename() const;          // returns 0 or 1 element path

  bool    root() const { return pathname_.size() == 1 && pathname_.at(0) == kPreferredSeparator; }
  bool    empty() const { return pathname_.empty(); }  // name consistent with std containers
  bool    has_parent_path() const     { return !parent_path().empty(); }
  bool    has_filename() const        { return !pathname_.empty(); }

  friend  std::ostream& operator<<(std::ostream& o, const Path& v);

 private:
  std::string pathname_;
};

inline bool operator==(const Path& lhs, const Path& rhs)        {return lhs.compare(rhs) == 0;}
inline bool operator==(const Path& lhs, const std::string& rhs) {return lhs.compare(rhs) == 0;}
inline bool operator==(const std::string& lhs, const Path& rhs) {return rhs.compare(lhs) == 0;}

inline bool operator!=(const Path& lhs, const Path& rhs)        {return lhs.compare(rhs) != 0;}
inline bool operator!=(const Path& lhs, const std::string& rhs) {return lhs.compare(rhs) != 0;}
inline bool operator!=(const std::string& lhs, const Path& rhs) {return rhs.compare(lhs) != 0;}

inline bool operator<(const Path& lhs, const Path& rhs)  {return lhs.compare(rhs) < 0;}
inline bool operator<=(const Path& lhs, const Path& rhs) {return !(rhs < lhs);}
inline bool operator> (const Path& lhs, const Path& rhs) {return rhs < lhs;}
inline bool operator>=(const Path& lhs, const Path& rhs) {return !(lhs < rhs);}


}  // namespace fs
}  // namespace foedus
#endif  // FOEDUS_FS_PATH_HPP_
