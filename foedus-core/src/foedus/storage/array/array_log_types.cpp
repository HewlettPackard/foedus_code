/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/array/array_log_types.hpp>
#include <algorithm>
#include <ostream>
namespace foedus {
namespace storage {
namespace array {
std::ostream& operator<<(std::ostream& o, const OverwriteLogType& v) {
    o << "<offset_>" << v.offset_ << "</offset_>"
        << "<payload_offset_>" << v.payload_offset_ << "</payload_offset_>"
        << "<payload_count_>" << v.payload_count_ << "</payload_count_>";
    // show first few bytes
    o << "<data_>";
    for (uint16_t i = 0; i < std::min<uint16_t>(8, v.payload_count_); ++i) {
        o << i << ":" << static_cast<int>(v.payload_[i]) << " ";
    }
    o << "...</data_>";
    return o;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
