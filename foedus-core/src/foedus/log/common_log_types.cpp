/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/common_log_types.hpp>
#include <ostream>
namespace foedus {
namespace log {
std::ostream& operator<<(std::ostream& o, const LogHeader& v) {
    o << "<Header>" << "<log_type_code_>" << v.log_type_code_ << "</log_type_code_>"
        << "<log_length_>" << v.log_length_ << "</log_length_>"
        << "<storage_id_>" << v.storage_id_ << "</storage_id_>" << "</Header>";
    return o;
}
}  // namespace log
}  // namespace foedus
