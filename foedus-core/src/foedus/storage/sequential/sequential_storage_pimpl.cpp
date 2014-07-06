/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"

#include <glog/logging.h>

#include <cstring>
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

void SequentialStorage::apply_append_record(
  thread::Thread* context,
  const SequentialAppendLogType* log_entry) {
  pimpl_->apply_append_record(context, log_entry);
}

SequentialStoragePimpl::SequentialStoragePimpl(
  Engine* engine,
  SequentialStorage* holder,
  const SequentialMetadata &metadata,
  bool create)
  :
    engine_(engine),
    holder_(holder),
    metadata_(metadata),
    volatile_list_(engine, metadata.id_),
    exist_(!create) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
}

ErrorStack SequentialStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an sequential-storage " << *holder_ << " exists=" << exist_;
  CHECK_ERROR(volatile_list_.initialize());

  if (exist_) {
    // TODO(Hideaki): initialize head_root_page_id_
  }
  return kRetOk;
}

ErrorStack SequentialStoragePimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing an sequential-storage " << *holder_;
  return volatile_list_.uninitialize();
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
  if (payload_count >= kMaxPayload) {
    return kErrorCodeStrTooLongPayload;
  }

  // Sequential storage doesn't need to check its current state for appends.
  // we are sure we can append it anyways, so we just create a log record.
  uint16_t log_length = SequentialAppendLogType::calculate_log_length(payload_count);
  SequentialAppendLogType* log_entry = reinterpret_cast<SequentialAppendLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, payload, payload_count);

  // also, we don't have to take a lock while commit because our SequentialVolatileList is
  // lock-free. So, we maintain a special lock-free write-set for sequential storage.
  return context->get_current_xct().add_to_lock_free_write_set(holder_, log_entry);
}

void SequentialStoragePimpl::apply_append_record(
  thread::Thread* context,
  const SequentialAppendLogType* log_entry) {
  volatile_list_.append_record(
    context,
    log_entry->header_.xct_id_,
    log_entry->payload_,
    log_entry->payload_count_);
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
