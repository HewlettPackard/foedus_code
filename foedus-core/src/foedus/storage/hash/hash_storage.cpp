/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {
ErrorStack  HashStorage::create(const Metadata &metadata) {
  return HashStoragePimpl(this).create(dynamic_cast<const HashMetadata&>(metadata));
}
ErrorStack  HashStorage::drop() { return HashStoragePimpl(this).drop(); }

void HashStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<HashStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "<bin_bits>" << static_cast<int>(control_block_->meta_.bin_bits_) << "</bin_bits>"
    << "</HashStorage>";
}
/* TODO(Hideaki) During surgery
void HashStorageFactory::add_create_log(const Metadata* metadata, thread::Thread* context) const {
  const HashMetadata* casted = dynamic_cast<const HashMetadata*>(metadata);
  ASSERT_ND(casted);

  uint16_t log_length = HashCreateLogType::calculate_log_length(casted->name_.size());
  HashCreateLogType* log_entry = reinterpret_cast<HashCreateLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    casted->id_,
    casted->name_.size(),
    casted->name_.data(),
    casted->bin_bits_);
}
*/
// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace hash
}  // namespace storage
}  // namespace foedus
