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
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

void MasstreeCreateLogType::populate(
  StorageId storage_id,
  uint16_t border_early_split_threshold,
  uint16_t name_length,
  const char* name) {
  ASSERT_ND(storage_id > 0);
  ASSERT_ND(name_length > 0);
  ASSERT_ND(name);
  header_.log_type_code_ = log::kLogCodeMasstreeCreate;
  header_.log_length_ = calculate_log_length(name_length);
  header_.storage_id_ = storage_id;
  border_early_split_threshold_ = border_early_split_threshold;
  name_length_ = name_length;
  std::memcpy(name_, name, name_length);
}
void MasstreeCreateLogType::apply_storage(Engine* engine, StorageId storage_id) {
  ASSERT_ND(storage_id > 0);
  LOG(INFO) << "Applying CREATE MASSTREE STORAGE log: " << *this;
  StorageName name(name_, name_length_);
  MasstreeMetadata metadata(header_.storage_id_, name, border_early_split_threshold_);
  engine->get_storage_manager()->create_storage_apply(&metadata);
  LOG(INFO) << "Applied CREATE MASSTREE STORAGE log: " << *this;
}

void MasstreeCreateLogType::construct(const Metadata* metadata, void* buffer) {
  ASSERT_ND(metadata->type_ == kMasstreeStorage);
  const MasstreeMetadata* casted = static_cast<const MasstreeMetadata*>(metadata);
  MasstreeCreateLogType* log_entry = reinterpret_cast<MasstreeCreateLogType*>(buffer);
  log_entry->populate(
    casted->id_,
    casted->border_early_split_threshold_,
    casted->name_.size(),
    casted->name_.data());
}

void MasstreeCreateLogType::assert_valid() {
  assert_valid_generic();
  ASSERT_ND(header_.log_length_ == calculate_log_length(name_length_));
  ASSERT_ND(header_.get_type() == log::get_log_code<MasstreeCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const MasstreeCreateLogType& v) {
  o << "<MasstreeCreateLog>"
    << "<storage_id_>" << v.header_.storage_id_ << "</storage_id_>"
    << "<border_early_split_threshold__>"
      << v.border_early_split_threshold_ << "</border_early_split_threshold__>"
    << "<name_>" << StorageName(v.name_, v.name_length_) << "</name_>"
    << "<name_length_>" << v.name_length_ << "</name_length_>"
    << "</MasstreeCreateLog>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const MasstreeInsertLogType& v) {
  o << "<MasstreeInsertLogType>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.data_, v.key_length_) << "</key_>"
    << "<layer_>" << static_cast<int>(v.layer_) << "</layer_>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>"
    << "<payload_>" << assorted::Top(v.data_ + v.key_length_, v.payload_count_) << "</payload_>"
    << "</MasstreeInsertLogType>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const MasstreeDeleteLogType& v) {
  o << "<MasstreeDeleteLogType>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.data_, v.key_length_) << "</key_>"
    << "<layer_>" << static_cast<int>(v.layer_) << "</layer_>"
    << "</MasstreeDeleteLogType>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const MasstreeOverwriteLogType& v) {
  o << "<MasstreeOverwriteLog>"
    << "<key_length_>" << v.key_length_ << "</key_length_>"
    << "<key_>" << assorted::Top(v.data_, v.key_length_) << "</key_>"
    << "<layer_>" << static_cast<int>(v.layer_) << "</layer_>"
    << "<payload_offset_>" << v.payload_offset_ << "</payload_offset_>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>"
    << "<payload_>" << assorted::Top(v.data_ + v.key_length_, v.payload_count_) << "</payload_>"
    << "</MasstreeOverwriteLog>";
  return o;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
