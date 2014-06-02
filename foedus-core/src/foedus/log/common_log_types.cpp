/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/common_log_types.hpp>
#include <foedus/log/log_type.hpp>
#include <ostream>
namespace foedus {
namespace log {
std::ostream& operator<<(std::ostream& o, const LogHeader& v) {
    o << "<Header type=\"0x" << std::hex << std::uppercase << v.log_type_code_ << std::nouppercase
        << std::dec << "\" type_name=\"" << get_log_type_name(v.get_type()) << "\""
        << " length=\"0x" << std::hex << std::uppercase << v.log_length_ << std::nouppercase
        << std::dec << "\" storage_id=\"" << v.storage_id_ << "\" />";
    return o;
}
std::ostream& operator<<(std::ostream& o, const FillerLogType &v) {
    o << "<FillerLog>" << v.header_ << "</FillerLog>";
    return o;
}

std::ostream& operator<<(std::ostream& o, const EpochMarkerLogType& v) {
    o << "<EpochMarker>" << v.header_
        << "<old_epoch_>" << v.old_epoch_ << "</old_epoch_>"
        << "<new_epoch_>" << v.new_epoch_ << "</new_epoch_>" << "</EpochMarker>";
    return o;
}

void FillerLogType::populate(uint64_t size) {
    header_.storage_id_ = 0;
    header_.log_length_ = size;
    header_.log_type_code_ = get_log_code<FillerLogType>();
}

}  // namespace log
}  // namespace foedus
