/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
