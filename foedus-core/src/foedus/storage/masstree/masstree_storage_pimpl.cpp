/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

// Defines MasstreeStorage methods so that we can inline implementation calls
bool        MasstreeStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        MasstreeStorage::exists()           const  { return pimpl_->exist_; }
StorageId   MasstreeStorage::get_id()           const  { return pimpl_->metadata_.id_; }
const std::string& MasstreeStorage::get_name()  const  { return pimpl_->metadata_.name_; }
const Metadata* MasstreeStorage::get_metadata() const  { return &pimpl_->metadata_; }
const MasstreeMetadata* MasstreeStorage::get_masstree_metadata() const  {
  return &pimpl_->metadata_;
}

MasstreeStoragePimpl::MasstreeStoragePimpl(
  Engine* engine,
  MasstreeStorage* holder,
  const MasstreeMetadata &metadata,
  bool create)
  :
    engine_(engine),
    holder_(holder),
    metadata_(metadata),
    exist_(!create) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
}

ErrorStack MasstreeStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an masstree-storage " << *holder_ << " exists=" << exist_;

  if (exist_) {
    // TODO(Hideaki): initialize head_root_page_id_
  }
  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing an masstree-storage " << *holder_;
  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::create(thread::Thread* /*context*/) {
  if (exist_) {
    LOG(ERROR) << "This masstree-storage already exists: " << *holder_;
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  LOG(INFO) << "Newly created an masstree-storage " << *holder_;
  exist_ = true;
  engine_->get_storage_manager().get_pimpl()->register_storage(holder_);
  return kRetOk;
}

ErrorCode MasstreeStoragePimpl::get_record(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */,
  void* /* payload */,
  uint16_t* /* payload_capacity */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::get_record_part(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */,
  void* /* payload */,
  uint16_t /* payload_offset */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::get_record_primitive(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */,
  PAYLOAD* /* payload */,
  uint16_t /* payload_offset */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::get_record_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */,
  void* /* payload */,
  uint16_t* /* payload_capacity */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::get_record_part_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */,
  void* /* payload */,
  uint16_t /* payload_offset */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::get_record_primitive_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */,
  PAYLOAD* /* payload */,
  uint16_t /* payload_offset */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::insert_record(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */,
  const void* /* payload */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::insert_record_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */,
  const void* /* payload */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::delete_record(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::delete_record_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::overwrite_record(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */,
  const void* /* payload */,
  uint16_t /* payload_offset */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::overwrite_record_primitive(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */,
  PAYLOAD /* payload */,
  uint16_t /* payload_offset */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::overwrite_record_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */,
  const void* /* payload */,
  uint16_t /* payload_offset */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::overwrite_record_primitive_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */,
  PAYLOAD /* payload */,
  uint16_t /* payload_offset */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::increment_record(
  thread::Thread* /* context */,
  const char* /* key */,
  uint16_t /* key_length */,
  PAYLOAD* /* value */,
  uint16_t /* payload_offset */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::increment_record_normalized(
  thread::Thread* /* context */,
  NormalizedPrimitiveKey /* key */,
  PAYLOAD* /* value */,
  uint16_t /* payload_offset */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_1(x) template ErrorCode MasstreeStoragePimpl::get_record_primitive< x > \
  (thread::Thread* context, const char* key, uint16_t key_length, x* payload, \
    uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_1);

#define EXPIN_2(x) template ErrorCode \
  MasstreeStoragePimpl::get_record_primitive_normalized< x > \
  (thread::Thread* context, NormalizedPrimitiveKey key, x* payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_2);

#define EXPIN_3(x) template ErrorCode \
  MasstreeStoragePimpl::overwrite_record_primitive< x > \
  (thread::Thread* context, const char* key, uint16_t key_length, x payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_3);

#define EXPIN_4(x) template ErrorCode \
  MasstreeStoragePimpl::overwrite_record_primitive_normalized< x > \
  (thread::Thread* context, NormalizedPrimitiveKey key, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_4);

#define EXPIN_5(x) template ErrorCode MasstreeStoragePimpl::increment_record< x > \
  (thread::Thread* context, const char* key, uint16_t key_length, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);

#define EXPIN_6(x) template ErrorCode MasstreeStoragePimpl::increment_record_normalized< x > \
  (thread::Thread* context, NormalizedPrimitiveKey key, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_6);
// @endcond

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
