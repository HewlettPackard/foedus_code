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
#include "foedus/storage/hash/hash_log_types.hpp"

#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {

void HashCreateLogType::apply_storage(Engine* engine, StorageId storage_id) {
  reinterpret_cast<CreateLogType*>(this)->apply_storage(engine, storage_id);
}

void HashCreateLogType::assert_valid() {
  reinterpret_cast<CreateLogType*>(this)->assert_valid();
  ASSERT_ND(header_.log_length_ == sizeof(HashCreateLogType));
  ASSERT_ND(header_.get_type() == log::get_log_code<HashCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const HashCreateLogType& v) {
  o << "<HashCreateLog>" << v.metadata_ << "</HashCreateLog>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const HashInsertLogType& v) {
  o << "<HashInsertLogType>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.data_, v.key_length_) << "</key_>"
    << "<bin1_>" << v.bin1_ << "</bin1_>"
    << "<hashtag_>" << v.hashtag_ << "</hashtag_>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>"
    << "<payload_>" << assorted::Top(v.data_ + v.key_length_, v.payload_count_) << "</payload_>"
    << "</HashInsertLogType>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const HashDeleteLogType& v) {
  o << "<HashDeleteLogType>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.data_, v.key_length_) << "</key_>"
    << "<bin1_>" << v.bin1_ << "</bin1_>"
    << "</HashDeleteLogType>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const HashOverwriteLogType& v) {
  o << "<HashOverwriteLog>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.data_, v.key_length_) << "</key_>"
    << "<bin1_>" << v.bin1_ << "</bin1_>"
    << "<payload_offset_>" << v.payload_offset_ << "</payload_offset_>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>"
    << "<payload_>" << assorted::Top(v.data_ + v.key_length_, v.payload_count_) << "</payload_>"
    << "</HashOverwriteLog>";
  return o;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
