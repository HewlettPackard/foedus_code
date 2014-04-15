/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_access.hpp>
#include <foedus/storage/record.hpp>
#include <ostream>
namespace foedus {
namespace xct {
std::ostream& operator<<(std::ostream& o, const XctAccess& v) {
    o << "XctAccess: observed_owner_id=" << v.observed_owner_id_ << ", record_address=" << v.record_
        << ", record current_owner_id=" << v.record_->owner_id_;
    return o;
}
}  // namespace xct
}  // namespace foedus
