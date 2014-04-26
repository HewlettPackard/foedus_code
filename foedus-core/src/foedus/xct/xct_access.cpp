/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/xct/xct_access.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/storage/storage.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/log/log_type_invoke.hpp>
#include <ostream>
namespace foedus {
namespace xct {
std::ostream& operator<<(std::ostream& o, const XctAccess& v) {
    o << "XctAccess: storage=" << v.storage_->get_name()
        << "observed_owner_id=" << v.observed_owner_id_ << ", record_address=" << v.record_
        << ", record current_owner_id=" << v.record_->owner_id_;
    return o;
}
std::ostream& operator<<(std::ostream& o, const WriteXctAccess& v) {
    o << "WriteAccess log=";
    log::invoke_ostream(v.log_entry_, &o);
    o << ". base=" << static_cast<const XctAccess&>(v);
    return o;
}
}  // namespace xct
}  // namespace foedus
