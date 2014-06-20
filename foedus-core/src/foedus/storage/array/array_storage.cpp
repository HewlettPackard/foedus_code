/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/storage/array/array_log_types.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_storage_pimpl.hpp>
#include <foedus/thread/thread.hpp>
#include <glog/logging.h>
#include <iostream>
#include <string>
namespace foedus {
namespace storage {
namespace array {
ArrayStorage::ArrayStorage(Engine* engine, const ArrayMetadata &metadata, bool create)
  : pimpl_(nullptr) {
  pimpl_ = new ArrayStoragePimpl(engine, this, metadata, create);
}
ArrayStorage::~ArrayStorage() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  ArrayStorage::initialize()              { return pimpl_->initialize(); }
ErrorStack  ArrayStorage::uninitialize()            { return pimpl_->uninitialize(); }
ErrorStack  ArrayStorage::create(thread::Thread* context)   { return pimpl_->create(context); }

void ArrayStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  o << "<ArrayStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "<payload_size>" << get_payload_size() << "</payload_size>"
    << "<array_size>" << get_array_size() << "</array_size>"
    << "</ArrayStorage>";
}

ErrorStack ArrayStorageFactory::get_instance(Engine* engine, const Metadata* metadata,
  Storage** storage) const {
  ASSERT_ND(metadata);
  const ArrayMetadata* casted = dynamic_cast<const ArrayMetadata*>(metadata);
  if (casted == nullptr) {
    LOG(INFO) << "WTF?? the metadata is null or not ArrayMetadata object";
    return ERROR_STACK(kErrorCodeStrWrongMetadataType);
  }

  if (casted->payload_size_ == 0) {
    // Array storage has no notion of insert/delete, thus payload=null doesn't make sense.
    LOG(INFO) << "Empty payload is not allowed for array storage";
    return ERROR_STACK(kErrorCodeStrArrayInvalidOption);
  } else if (casted->array_size_ == 0) {
    LOG(INFO) << "Empty array is not allowed for array storage";
    return ERROR_STACK(kErrorCodeStrArrayInvalidOption);
  }

  *storage = new ArrayStorage(engine, *casted, false);
  return kRetOk;
}
bool ArrayStorageFactory::is_right_metadata(const Metadata *metadata) const {
  return dynamic_cast<const ArrayMetadata*>(metadata) != nullptr;
}

void ArrayStorageFactory::add_create_log(const Metadata* metadata, thread::Thread* context) const {
  const ArrayMetadata* casted = dynamic_cast<const ArrayMetadata*>(metadata);
  ASSERT_ND(casted);

  uint16_t log_length = CreateLogType::calculate_log_length(casted->name_.size());
  CreateLogType* log_entry = reinterpret_cast<CreateLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    casted->id_,
    casted->array_size_,
    casted->payload_size_,
    casted->name_.size(),
    casted->name_.data());
}


// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace array
}  // namespace storage
}  // namespace foedus
