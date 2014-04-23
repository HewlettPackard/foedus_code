/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <ostream>
#include <string>
namespace foedus {
namespace fs {
Path::Path(const std::string& s) {
    if (s.size() > 0 && s.at(0) == '~'
        && (s.size() == 1 || s.at(1) == '/')) {
        // starts with ~/ or ~ alone: resolve it as home folder
        pathname_ = home_path().pathname_;
        pathname_.append(s.substr(1));
    } else {
        if (s.empty() || s.at(0) == PREFERRED_SEPARATOR) {
            pathname_ = s;
        } else {
            Path tmp = current_path();
            tmp /= s;
            pathname_ = tmp.string();
        }
    }
}

Path Path::filename() const {
    size_t pos = pathname_.find_last_of(PREFERRED_SEPARATOR);
    if (pos == pathname_.size()) {
        return Path(pathname_);
    } else {
        return Path(pathname_.c_str() + pos);
    }
}

Path Path::parent_path() const {
    size_t pos = pathname_.find_last_of(PREFERRED_SEPARATOR);
    if (pos == pathname_.size()) {
        return Path();
    } else {
        return Path(pathname_.substr(0, pos));
    }
}

std::ostream& operator<<(std::ostream& o, const Path& v) {
    o << v.string();
    return o;
}

}  // namespace fs
}  // namespace foedus

