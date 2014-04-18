/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/xct/xct_options.hpp>
#include <ostream>
namespace foedus {
namespace xct {
XctOptions::XctOptions() {
    max_read_set_size_ = DEFAULT_MAX_READ_SET_SIZE;
    max_write_set_size_ = DEFAULT_MAX_WRITE_SET_SIZE;
}

std::ostream& operator<<(std::ostream& o, const XctOptions& v) {
    o << "  <XctOptions>" << std::endl;
    EXTERNALIZE_WRITE(max_read_set_size_);
    EXTERNALIZE_WRITE(max_write_set_size_);
    o << "  </XctOptions>" << std::endl;
    return o;
}
}  // namespace xct
}  // namespace foedus
