/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace sequential {
SequentialStorage::SequentialStorage(
  Engine* engine, const SequentialMetadata &metadata, bool create)
  : pimpl_(nullptr) {
  pimpl_ = new SequentialStoragePimpl(engine, this, metadata, create);
}
SequentialStorage::~SequentialStorage() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  SequentialStorage::initialize()              { return pimpl_->initialize(); }
ErrorStack  SequentialStorage::uninitialize()            { return pimpl_->uninitialize(); }
ErrorStack  SequentialStorage::create(thread::Thread* context)   { return pimpl_->create(context); }

void SequentialStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<SequentialStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << pimpl_->volatile_list_
    << "</SequentialStorage>";
}

ErrorStack SequentialStorageFactory::get_instance(Engine* engine, const Metadata* metadata,
  Storage** storage) const {
  ASSERT_ND(metadata);
  const SequentialMetadata* casted = dynamic_cast<const SequentialMetadata*>(metadata);
  if (casted == nullptr) {
    LOG(INFO) << "WTF?? the metadata is null or not SequentialMetadata object";
    return ERROR_STACK(kErrorCodeStrWrongMetadataType);
  }

  *storage = new SequentialStorage(engine, *casted, false);
  return kRetOk;
}
bool SequentialStorageFactory::is_right_metadata(const Metadata *metadata) const {
  return dynamic_cast<const SequentialMetadata*>(metadata) != nullptr;
}

void SequentialStorageFactory::add_create_log(
  const Metadata* metadata, thread::Thread* context) const {
  const SequentialMetadata* casted = dynamic_cast<const SequentialMetadata*>(metadata);
  ASSERT_ND(casted);

  uint16_t log_length = SequentialCreateLogType::calculate_log_length(casted->name_.size());
  SequentialCreateLogType* log_entry = reinterpret_cast<SequentialCreateLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    casted->id_,
    casted->name_.size(),
    casted->name_.data());
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
