/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_log_types.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "foedus/assert_nd.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace sequential {

void SequentialCreateLogType::populate(
  StorageId storage_id,
  uint16_t name_length,
  const char* name) {
  ASSERT_ND(storage_id > 0);
  ASSERT_ND(name_length > 0);
  ASSERT_ND(name);
  header_.log_type_code_ = log::kLogCodeSequentialCreate;
  header_.log_length_ = calculate_log_length(name_length);
  header_.storage_id_ = storage_id;
  name_length_ = name_length;
  std::memcpy(name_, name, name_length);
}
void SequentialCreateLogType::apply_storage(thread::Thread* context, Storage* storage) {
  ASSERT_ND(storage == nullptr);  // because we are now creating it.
  LOG(INFO) << "Applying CREATE SEQUENTIAL STORAGE log: " << *this;
  std::string name(name_, name_length_);
  SequentialMetadata metadata(header_.storage_id_, name, 0);
  std::unique_ptr<SequentialStorage> sequential(
    new SequentialStorage(context->get_engine(), metadata, true));
  COERCE_ERROR(sequential->initialize());
  COERCE_ERROR(sequential->create(context));
  sequential.release();  // No error, so take over the ownership from unique_ptr.
  LOG(INFO) << "Applied CREATE SEQUENTIAL STORAGE log: " << *this;
}

void SequentialCreateLogType::assert_valid() {
  assert_valid_generic();
  ASSERT_ND(header_.log_length_ == calculate_log_length(name_length_));
  ASSERT_ND(header_.get_type() == log::get_log_code<SequentialCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const SequentialCreateLogType& v) {
  o << "<SequentialCreateLog>"
    << "<storage_id_>" << v.header_.storage_id_ << "</storage_id_>"
    << "<name_>" << std::string(v.name_, v.name_length_) << "</name_>"
    << "<name_length_>" << v.name_length_ << "</name_length_>"
    << "</SequentialCreateLog>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const SequentialAppendLogType& v) {
  o << "<SequentialAppendLog>"
    << "<payload_count_>" << v.payload_count_ << "</payload_count_>";
  // show first few bytes
  o << "<data_>";
  for (uint16_t i = 0; i < std::min<uint16_t>(8, v.payload_count_); ++i) {
    o << i << ":" << static_cast<int>(v.payload_[i]) << " ";
  }
  o << "...</data_>";
  o << "</SequentialAppendLog>";
  return o;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
