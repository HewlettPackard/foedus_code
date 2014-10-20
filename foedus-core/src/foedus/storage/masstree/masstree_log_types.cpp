/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_log_types.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

void MasstreeCreateLogType::apply_storage(Engine* engine, StorageId storage_id) {
  reinterpret_cast<CreateLogType*>(this)->apply_storage(engine, storage_id);
}

void MasstreeCreateLogType::assert_valid() {
  reinterpret_cast<CreateLogType*>(this)->assert_valid();
  ASSERT_ND(header_.log_length_ == sizeof(MasstreeCreateLogType));
  ASSERT_ND(header_.get_type() == log::get_log_code<MasstreeCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const MasstreeCreateLogType& v) {
  o << "<MasstreeCreateLog>" << v.metadata_ << "</MasstreeCreateLog>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const MasstreeInsertLogType& v) {
  o << "<MasstreeInsertLogType>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.get_key(), v.key_length_) << "</key_>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>"
    << "<payload_>" << assorted::Top(v.get_payload(), v.payload_count_) << "</payload_>"
    << "</MasstreeInsertLogType>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const MasstreeDeleteLogType& v) {
  o << "<MasstreeDeleteLogType>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.get_key(), v.key_length_) << "</key_>"
    << "</MasstreeDeleteLogType>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const MasstreeOverwriteLogType& v) {
  o << "<MasstreeOverwriteLog>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.get_key(), v.key_length_) << "</key_>"
    << "<payload_offset_>" << v.payload_offset_ << "</payload_offset_>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>"
    << "<payload_>" << assorted::Top(v.get_payload(), v.payload_count_) << "</payload_>"
    << "</MasstreeOverwriteLog>";
  return o;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
