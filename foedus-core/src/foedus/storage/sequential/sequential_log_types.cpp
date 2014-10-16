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
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace sequential {

void SequentialCreateLogType::apply_storage(Engine* engine, StorageId storage_id) {
  reinterpret_cast<CreateLogType*>(this)->apply_storage(engine, storage_id);
}

void SequentialCreateLogType::assert_valid() {
  reinterpret_cast<CreateLogType*>(this)->assert_valid();
  ASSERT_ND(header_.log_length_ == sizeof(SequentialCreateLogType));
  ASSERT_ND(header_.get_type() == log::get_log_code<SequentialCreateLogType>());
}
std::ostream& operator<<(std::ostream& o, const SequentialCreateLogType& v) {
  o << "<SequentialCreateLog>" << v.metadata_ << "</SequentialCreateLog>";
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
