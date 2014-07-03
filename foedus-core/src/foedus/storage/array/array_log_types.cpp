/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_log_types.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "foedus/assert_nd.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace array {

void ArrayCreateLogType::populate(
  StorageId storage_id,
  ArrayOffset array_size,
  uint16_t payload_size,
  uint16_t name_length,
  const char* name) {
  ASSERT_ND(storage_id > 0);
  ASSERT_ND(array_size > 0);
  ASSERT_ND(name_length > 0);
  ASSERT_ND(name);
  header_.log_type_code_ = log::kLogCodeArrayCreate;
  header_.log_length_ = calculate_log_length(name_length);
  header_.storage_id_ = storage_id;
  array_size_ = array_size;
  payload_size_ = payload_size;
  name_length_ = name_length;
  std::memcpy(name_, name, name_length);
}
void ArrayCreateLogType::apply_storage(thread::Thread* context, Storage* storage) {
  ASSERT_ND(storage == nullptr);  // because we are now creating it.
  LOG(INFO) << "Applying CREATE ARRAY STORAGE log: " << *this;
  std::string name(name_, name_length_);
  ArrayMetadata metadata(header_.storage_id_, name, payload_size_, array_size_, 0);
  std::unique_ptr<array::ArrayStorage> array(new array::ArrayStorage(context->get_engine(),
    metadata, true));
  COERCE_ERROR(array->initialize());
  COERCE_ERROR(array->create(context));
  array.release();  // No error, so take over the ownership from unique_ptr.
  LOG(INFO) << "Applied CREATE ARRAY STORAGE log: " << *this;
}

void ArrayCreateLogType::assert_valid() {
  assert_valid_generic();
  ASSERT_ND(header_.log_length_ == calculate_log_length(name_length_));
  ASSERT_ND(header_.get_type() == log::get_log_code<ArrayCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const ArrayCreateLogType& v) {
  o << "<ArrayCreateLog>"
    << "<storage_id_>" << v.header_.storage_id_ << "</storage_id_>"
    << "<name_>" << std::string(v.name_, v.name_length_) << "</name_>"
    << "<name_length_>" << v.name_length_ << "</name_length_>"
    << "<array_size_>" << v.array_size_ << "</array_size_>"
    << "</ArrayCreateLog>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const ArrayOverwriteLogType& v) {
  o << "<ArrayOverwriteLog>"
    << "<offset_>" << v.offset_ << "</offset_>"
    << "<payload_offset_>" << v.payload_offset_ << "</payload_offset_>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>";
  // show first few bytes
  o << "<data_>";
  for (uint16_t i = 0; i < std::min<uint16_t>(8, v.payload_count_); ++i) {
    o << i << ":" << static_cast<int>(v.payload_[i]) << " ";
  }
  o << "...</data_>";
  o << "</ArrayOverwriteLog>";
  return o;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
