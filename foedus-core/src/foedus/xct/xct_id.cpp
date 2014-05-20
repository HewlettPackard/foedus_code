/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_id.hpp>
#include <ostream>
namespace foedus {
namespace xct {
std::ostream& operator<<(std::ostream& o, const XctId& v) {
    o << "<XctId><epoch>" << v.data_.components.epoch << "</epoch><thread_id>"
        << v.data_.components.thread_id << "</thread_id>"
        << "<ordinal>" << (v.data_.components.ordinal_and_status & 0x7FFF) << "</ordinal>"
        << "<locked>" << (v.is_locked()) << "</locked></XctId>";
    return o;
}
}  // namespace xct
}  // namespace foedus
