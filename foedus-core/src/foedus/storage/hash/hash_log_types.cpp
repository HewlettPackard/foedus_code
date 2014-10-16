/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
