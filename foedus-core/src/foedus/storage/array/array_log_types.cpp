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
void ArrayCreateLogType::apply_storage(thread::Thread* context, StorageId storage_id) {
  /* TODO(Hideaki) During surgery
  ASSERT_ND(storage == nullptr);  // because we are now creating it.
  LOG(INFO) << "Applying CREATE ARRAY STORAGE log: " << *this;
  StorageName name(name_, name_length_);
  ArrayMetadata metadata(header_.storage_id_, name, payload_size_, array_size_);
  std::unique_ptr<array::ArrayStorage> array(new array::ArrayStorage(context->get_engine(),
    metadata, true));
  COERCE_ERROR(array->initialize());
  COERCE_ERROR(array->create(context));
  array.release();  // No error, so take over the ownership from unique_ptr.
  LOG(INFO) << "Applied CREATE ARRAY STORAGE log: " << *this;
  */
}

void ArrayCreateLogType::assert_valid() {
  assert_valid_generic();
  ASSERT_ND(header_.log_length_ == calculate_log_length(name_length_));
  ASSERT_ND(header_.get_type() == log::get_log_code<ArrayCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const ArrayCreateLogType& v) {
  o << "<ArrayCreateLog>"
    << "<storage_id_>" << v.header_.storage_id_ << "</storage_id_>"
    << "<name_>" << StorageName(v.name_, v.name_length_) << "</name_>"
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

// just to shutup pesky compiler
template <typename T>
inline T as(const void *address) {
  const T* casted = reinterpret_cast<const T*>(address);
  return *casted;
}

std::ostream& operator<<(std::ostream& o, const ArrayIncrementLogType& v) {
  o << "<ArrayIncrementLog>"
    << "<offset_>" << v.offset_ << "</offset_>"
    << "<payload_offset_>" << v.payload_offset_ << "</payload_offset_>"
    << "<type_>";
  switch (v.get_value_type()) {
    // 32 bit data types
    case kI8:
      o << "int8_t</type><addendum_>" << static_cast<int16_t>(as<int8_t>(v.addendum_));
      break;
    case kI16:
      o << "int16_t</type><addendum_>" << as<int16_t>(v.addendum_);
      break;
    case kI32:
      o << "int32_t</type><addendum_>" << as<int32_t>(v.addendum_);
      break;
    case kBool:
    case kU8:
      o << "uint8_t</type><addendum_>" << static_cast<uint16_t>(as<uint8_t>(v.addendum_));
      break;
    case kU16:
      o << "uint16_t</type><addendum_>" << as<uint16_t>(v.addendum_);
      break;
    case kU32:
      o << "uint32_t</type><addendum_>" << as<uint32_t>(v.addendum_);
      break;
    case kFloat:
      o << "float</type><addendum_>" << as<float>(v.addendum_);
      break;

    // 64 bit data types
    case kI64:
      o << "int64_t</type><addendum_>" << as<int64_t>(v.addendum_ + 4);
      break;
    case kU64:
      o << "uint64_t</type><addendum_>" << as<uint64_t>(v.addendum_ + 4);
      break;
    case kDouble:
      o << "double</type><addendum_>" << as<double>(v.addendum_ + 4);
      break;
    default:
      o << "UNKNOWN(" << v.get_value_type() << ")</type><addendum_>";
      ASSERT_ND(false);
      break;
  }
  o << "</addendum_>";
  o << "</ArrayIncrementLog>";
  return o;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
