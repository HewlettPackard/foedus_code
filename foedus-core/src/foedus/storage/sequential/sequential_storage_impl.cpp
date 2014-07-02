/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"

#include <glog/logging.h>

#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_inl.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace sequential {

// Defines SequentialStorage methods so that we can inline implementation calls
bool        SequentialStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        SequentialStorage::exists()           const  { return pimpl_->exist_; }
StorageId   SequentialStorage::get_id()           const  { return pimpl_->metadata_.id_; }
const std::string& SequentialStorage::get_name()  const  { return pimpl_->metadata_.name_; }
const Metadata* SequentialStorage::get_metadata() const  { return &pimpl_->metadata_; }
const SequentialMetadata* SequentialStorage::get_sequential_metadata() const  {
  return &pimpl_->metadata_;
}

ErrorCode SequentialStorage::append_record(
  thread::Thread* context,
  const void *payload,
  uint16_t payload_count) {
  return pimpl_->append_record(context, payload, payload_count);
}

SequentialStoragePimpl::SequentialStoragePimpl(
  Engine* engine,
  SequentialStorage* holder,
  const SequentialMetadata &metadata,
  bool create)
  : engine_(engine), holder_(holder), metadata_(metadata), exist_(!create) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
}

ErrorStack SequentialStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an sequential-storage " << *holder_ << " exists=" << exist_;

  if (exist_) {
    // initialize root_page_
  }
  return kRetOk;
}

void SequentialStoragePimpl::release_pages_recursive(
  memory::PageReleaseBatch* /*batch*/,
  SequentialPage* /*page*/,
  VolatilePagePointer /*volatile_page_id*/) {
}

ErrorStack SequentialStoragePimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing an sequential-storage " << *holder_;
  return kRetOk;
}

ErrorStack SequentialStoragePimpl::create(thread::Thread* /*context*/) {
  if (exist_) {
    LOG(ERROR) << "This sequential-storage already exists: " << *holder_;
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  LOG(INFO) << "Newly created an sequential-storage " << *holder_;
  exist_ = true;
  engine_->get_storage_manager().get_pimpl()->register_storage(holder_);
  return kRetOk;
}

inline ErrorCode SequentialStoragePimpl::append_record(
  thread::Thread* context, const void *payload, uint16_t payload_count) {
  ASSERT_ND(payload_count < SequentialPage::kMaxPayload);
  Record *record = nullptr;
  // CHECK_ERROR_CODE(locate_record(context, offset, &record));

  // write out log
  uint16_t log_length = AppendLogType::calculate_log_length(payload_count);
  AppendLogType* log_entry = reinterpret_cast<AppendLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, payload, payload_count);
  return context->get_current_xct().add_to_write_set(holder_, record, log_entry);
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
