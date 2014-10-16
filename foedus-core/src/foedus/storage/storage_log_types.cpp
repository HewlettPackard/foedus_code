/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/storage_log_types.hpp"

#include <glog/logging.h>

#include <ostream>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {

void DropLogType::populate(StorageId storage_id) {
  ASSERT_ND(storage_id > 0);
  header_.log_type_code_ = log::get_log_code<DropLogType>();
  header_.log_length_ = sizeof(DropLogType);
  header_.storage_id_ = storage_id;
}
void DropLogType::apply_storage(Engine* engine, StorageId storage_id) {
  ASSERT_ND(storage_id > 0);
  LOG(INFO) << "Applying DROP STORAGE log: " << *this;
  engine->get_storage_manager()->drop_storage_apply(storage_id);
  LOG(INFO) << "Applied DROP STORAGE log: " << *this;
}

void DropLogType::assert_valid() {
  assert_valid_generic();
  ASSERT_ND(header_.log_length_ == sizeof(DropLogType));
  ASSERT_ND(header_.get_type() == log::get_log_code<DropLogType>());
}
std::ostream& operator<<(std::ostream& o, const DropLogType& v) {
  o << "<StorageDropLog>"
    << "<storage_id_>" << v.header_.storage_id_ << "</storage_id_>"
    << "</StorageDropLog>";
  return o;
}

void CreateLogType::assert_valid() {
  assert_valid_generic();
  ASSERT_ND(header_.storage_id_ == metadata_.id_);
}

void CreateLogType::apply_storage(Engine* engine, StorageId storage_id) {
  ASSERT_ND(metadata_.id_ == storage_id);
  LOG(INFO) << "Applying CREATE STORAGE log: " << *this;
  engine->get_storage_manager()->create_storage_apply(metadata_);
  LOG(INFO) << "Applied CREATE STORAGE log: " << *this;
}

std::ostream& operator<<(std::ostream& o, const CreateLogType& v) {
  switch (v.metadata_.type_) {
    case kArrayStorage:
      o << reinterpret_cast<const array::ArrayCreateLogType&>(v);
      break;
    case kHashStorage:
      o << reinterpret_cast<const hash::HashCreateLogType&>(v);
      break;
    case kMasstreeStorage:
      o << reinterpret_cast<const masstree::MasstreeCreateLogType&>(v);
      break;
    case kSequentialStorage:
      o << reinterpret_cast<const sequential::SequentialCreateLogType&>(v);
      break;
    default:
      o << "Unexpected metadata type:" << v.metadata_.type_;
  }
  return o;
}



}  // namespace storage
}  // namespace foedus
