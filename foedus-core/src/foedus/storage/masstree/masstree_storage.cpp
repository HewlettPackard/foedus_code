/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {
MasstreeStorage::MasstreeStorage(
  Engine* engine, const MasstreeMetadata &metadata, bool create)
  : pimpl_(nullptr) {
  pimpl_ = new MasstreeStoragePimpl(engine, this, metadata, create);
}
MasstreeStorage::~MasstreeStorage() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  MasstreeStorage::initialize()              { return pimpl_->initialize(); }
ErrorStack  MasstreeStorage::uninitialize()            { return pimpl_->uninitialize(); }
ErrorStack  MasstreeStorage::create(thread::Thread* context)   { return pimpl_->create(context); }

void MasstreeStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<MasstreeStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "</MasstreeStorage>";
}

ErrorStack MasstreeStorageFactory::get_instance(Engine* engine, const Metadata* metadata,
  Storage** storage) const {
  ASSERT_ND(metadata);
  const MasstreeMetadata* casted = dynamic_cast<const MasstreeMetadata*>(metadata);
  if (casted == nullptr) {
    LOG(INFO) << "WTF?? the metadata is null or not MasstreeMetadata object";
    return ERROR_STACK(kErrorCodeStrWrongMetadataType);
  }

  *storage = new MasstreeStorage(engine, *casted, false);
  return kRetOk;
}
bool MasstreeStorageFactory::is_right_metadata(const Metadata *metadata) const {
  return dynamic_cast<const MasstreeMetadata*>(metadata) != nullptr;
}

void MasstreeStorageFactory::add_create_log(
  const Metadata* metadata, thread::Thread* context) const {
  const MasstreeMetadata* casted = dynamic_cast<const MasstreeMetadata*>(metadata);
  ASSERT_ND(casted);

  uint16_t log_length = MasstreeCreateLogType::calculate_log_length(casted->name_.size());
  MasstreeCreateLogType* log_entry = reinterpret_cast<MasstreeCreateLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    casted->id_,
    casted->name_.size(),
    casted->name_.data());
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
