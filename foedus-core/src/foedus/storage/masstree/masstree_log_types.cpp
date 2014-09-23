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
#include "foedus/assorted/endianness.hpp"
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
void MasstreeCreateLogType::apply_storage(thread::Thread* context, StorageId storage_id) {
  /* TODO(Hideaki) During surgery
  ASSERT_ND(storage == nullptr);  // because we are now creating it.
  LOG(INFO) << "Applying CREATE MASSTREE STORAGE log: " << *this;
  StorageName name(name_, name_length_);
  MasstreeMetadata metadata(header_.storage_id_, name, border_early_split_threshold_);
  std::unique_ptr<MasstreeStorage> masstree(
    new MasstreeStorage(context->get_engine(), metadata, true));
  COERCE_ERROR(masstree->initialize());
  COERCE_ERROR(masstree->create(context));
  masstree.release();  // No error, so take over the ownership from unique_ptr.
  LOG(INFO) << "Applied CREATE MASSTREE STORAGE log: " << *this;
  */
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
