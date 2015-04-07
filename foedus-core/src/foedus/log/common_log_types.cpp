/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include "foedus/log/common_log_types.hpp"

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/logger_impl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace log {
std::ostream& operator<<(std::ostream& o, const LogHeader& v) {
  o << "<Header type=\"" << assorted::Hex(v.log_type_code_) << "\" type_name=\""
    << get_log_type_name(v.get_type()) << "\" length=\"" << assorted::Hex(v.log_length_)
    << "\" storage_id=\"" << v.storage_id_ << "\" />";
  return o;
}
std::ostream& operator<<(std::ostream& o, const FillerLogType &v) {
  o << "<FillerLog>" << v.header_ << "</FillerLog>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const EpochMarkerLogType& v) {
  o << "<EpochMarker>" << v.header_
    << "<old_epoch_>" << v.old_epoch_ << "</old_epoch_>"
    << "<new_epoch_>" << v.new_epoch_ << "</new_epoch_>"
    << "<logger_numa_node_>" << static_cast<int>(v.logger_numa_node_) << "</logger_numa_node_>"
    << "<logger_in_node_ordinal_>"
      << static_cast<int>(v.logger_in_node_ordinal_) << "</logger_in_node_ordinal_>"
    << "<logger_id_>" << v.logger_id_ << "</logger_id_>"
    << "<log_file_ordinal_>" << v.log_file_ordinal_ << "</log_file_ordinal_>"
    << "<log_file_offset_>" << assorted::Hex(v.log_file_offset_) << "</log_file_offset_>"
    << "</EpochMarker>";
  return o;
}

void EpochMarkerLogType::apply_engine(thread::Thread* context) {
  log::LoggerRef logger = context->get_engine()->get_log_manager()->get_logger(logger_id_);
  logger.add_epoch_history(*this);
}

void EpochMarkerLogType::populate(Epoch old_epoch, Epoch new_epoch,
          uint8_t logger_numa_node, uint8_t logger_in_node_ordinal,
          uint16_t logger_id, uint32_t log_file_ordinal, uint64_t log_file_offset) {
  header_.storage_id_ = 0;
  header_.log_length_ = sizeof(EpochMarkerLogType);
  header_.log_type_code_ = get_log_code<EpochMarkerLogType>();
  new_epoch_ = new_epoch;
  old_epoch_ = old_epoch;
  logger_numa_node_ = logger_numa_node;
  logger_in_node_ordinal_ = logger_in_node_ordinal;
  logger_id_ = logger_id;
  log_file_ordinal_ = log_file_ordinal;
  log_file_offset_ = log_file_offset;
  assert_valid();
}

void FillerLogType::populate(uint64_t size) {
  ASSERT_ND(size < (1 << 16));
  header_.storage_id_ = 0;
  header_.log_length_ = size;
  header_.log_type_code_ = get_log_code<FillerLogType>();
}

}  // namespace log
}  // namespace foedus
