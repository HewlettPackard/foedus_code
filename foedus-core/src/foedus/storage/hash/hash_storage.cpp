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
HashStorage::HashStorage(Engine* engine, const HashMetadata &metadata, bool create)
  : pimpl_(get_pimpl_memory_casted<HashStoragePimpl>(engine, metadata.id_)) {
  ASSERT_ND(sizeof(HashStoragePimpl) <= kPageSize);
  new (pimpl_) HashStoragePimpl(engine, this, metadata, create);
}
HashStorage::~HashStorage() {
  pimpl_->~HashStoragePimpl();
}

ErrorStack  HashStorage::initialize()              { return pimpl_->initialize(); }
ErrorStack  HashStorage::uninitialize()            { return pimpl_->uninitialize(); }
ErrorStack  HashStorage::create(thread::Thread* context)   { return pimpl_->create(context); }

void HashStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<HashStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "<bin_bits>" << static_cast<int>(pimpl_->metadata_.bin_bits_) << "</bin_bits>"
    << "</HashStorage>";
}

ErrorStack HashStorageFactory::get_instance(Engine* engine, const Metadata* metadata,
  Storage** storage) const {
  ASSERT_ND(metadata);
  const HashMetadata* casted = dynamic_cast<const HashMetadata*>(metadata);
  if (casted == nullptr) {
    LOG(INFO) << "WTF?? the metadata is null or not HashMetadata object";
    return ERROR_STACK(kErrorCodeStrWrongMetadataType);
  }

  *storage = new HashStorage(engine, *casted, false);
  return kRetOk;
}
bool HashStorageFactory::is_right_metadata(const Metadata *metadata) const {
  return dynamic_cast<const HashMetadata*>(metadata) != nullptr;
}

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

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace hash
}  // namespace storage
}  // namespace foedus
