/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/fs/path.hpp>
#include <ostream>
#include <string>
namespace foedus {
namespace fs {
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

}  // namespace fs
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::fs::Path& v) {
    o << v.string();
    return o;
}
