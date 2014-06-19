/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/log/epoch_history.hpp>
#include <ostream>
namespace foedus {
namespace log {
std::ostream& operator<<(std::ostream& o, const EpochHistory& v) {
  o << "<EpochHistory old_epoch=\"" << v.old_epoch_ << "\" new_epoch=\"" << v.new_epoch_
    << "\" log_file_ordinal=\"" << v.log_file_ordinal_
    << "\" log_file_offset_=\"" << assorted::Hex(v.log_file_offset_) << "\" />";
  return o;
}
}  // namespace log
}  // namespace foedus
