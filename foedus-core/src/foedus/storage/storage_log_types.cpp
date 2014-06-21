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
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {

void DropLogType::populate(StorageId storage_id) {
  ASSERT_ND(storage_id > 0);
  header_.log_type_code_ = log::get_log_code<DropLogType>();
  header_.log_length_ = sizeof(DropLogType);
  header_.storage_id_ = storage_id;
}
void DropLogType::apply_storage(const xct::XctId& /*xct_id*/,
                thread::Thread* context, Storage* storage) {
  ASSERT_ND(storage);  // because we are now dropping it.
  ASSERT_ND(header_.storage_id_ > 0);
  ASSERT_ND(header_.storage_id_ == storage->get_id());
  LOG(INFO) << "Applying DROP STORAGE log: " << *this;
  context->get_engine()->get_storage_manager().get_pimpl()->drop_storage_apply(context, storage);
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

}  // namespace storage
}  // namespace foedus
