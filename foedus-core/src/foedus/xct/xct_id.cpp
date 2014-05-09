/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_id.hpp>
#include <ostream>
namespace foedus {
namespace xct {
std::ostream& operator<<(std::ostream& o, const XctId& v) {
    o << "<XctId><epoch>" << v.epoch_ << "</epoch><thread_id>" << v.thread_id_ << "</thread_id>"
        << "<ordinal>" << (v.ordinal_and_status_ & 0x7FFF) << "</ordinal>"
        << "<locked>" << (v.is_locked<15>()) << "</locked></XctId>";
    return o;
}
}  // namespace xct
}  // namespace foedus
