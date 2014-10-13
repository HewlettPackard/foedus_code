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
#include "foedus/engine.hpp"
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace array {

void ArrayCreateLogType::apply_storage(Engine* engine, StorageId storage_id) {
  reinterpret_cast<CreateLogType*>(this)->apply_storage(engine, storage_id);
}

void ArrayCreateLogType::assert_valid() {
  reinterpret_cast<CreateLogType*>(this)->assert_valid();
  ASSERT_ND(header_.log_length_ == sizeof(ArrayCreateLogType));
  ASSERT_ND(header_.get_type() == log::get_log_code<ArrayCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const ArrayCreateLogType& v) {
  o << "<ArrayCreateLog>" << v.metadata_ << "</ArrayCreateLog>";
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
