/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_id.hpp>
#include <ostream>
namespace foedus {
namespace xct {
std::ostream& operator<<(std::ostream& o, const XctId& v) {
    o << "XctId: epoch=" << v.epoch_ << ", thread_id=" << v.thread_id_
        << ", ordinal_and_status_=" << v.ordinal_and_status_;
    return o;
}
}  // namespace xct
}  // namespace foedus
