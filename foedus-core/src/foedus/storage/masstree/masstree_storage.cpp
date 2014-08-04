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
#include "foedus/storage/masstree/masstree_retry_impl.hpp"
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

ErrorCode MasstreeStorage::get_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t* payload_capacity) {
  return masstree_retry([this, context, key, key_length, payload, payload_capacity]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record(context, key, key_length, false, &border, &index));
    return pimpl_->retrieve_general(
      context,
      border,
      index,
      key,
      key_length,
      payload,
      payload_capacity);
  });
}

ErrorCode MasstreeStorage::get_record_part(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return masstree_retry([this, context, key, key_length, payload, payload_offset, payload_count]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record(context, key, key_length, false, &border, &index));
    return pimpl_->retrieve_part_general(
      context,
      border,
      index,
      key,
      key_length,
      payload,
      payload_offset,
      payload_count);
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::get_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  return masstree_retry([this, context, key, key_length, payload, payload_offset]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record(context, key, key_length, false, &border, &index));
    return pimpl_->retrieve_part_general(
      context,
      border,
      index,
      key,
      key_length,
      payload,
      payload_offset,
      sizeof(PAYLOAD));
  });
}

ErrorCode MasstreeStorage::get_record_normalized(
  thread::Thread* context,
  KeySlice key,
  void* payload,
  uint16_t* payload_capacity) {
  return masstree_retry([this, context, key, payload, payload_capacity]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record_normalized(context, key, false, &border, &index));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->retrieve_general(
      context,
      border,
      index,
      &be_key,
      sizeof(be_key),
      payload,
      payload_capacity);
  });
}

ErrorCode MasstreeStorage::get_record_part_normalized(
  thread::Thread* context,
  KeySlice key,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return masstree_retry([this, context, key, payload, payload_offset, payload_count]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record_normalized(context, key, false, &border, &index));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->retrieve_part_general(
      context,
      border,
      index,
      &be_key,
      sizeof(be_key),
      payload,
      payload_offset,
      payload_count);
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::get_record_primitive_normalized(
  thread::Thread* context,
  KeySlice key,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  return masstree_retry([this, context, key, payload, payload_offset]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record_normalized(context, key, false, &border, &index));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->retrieve_part_general(
      context,
      border,
      index,
      &be_key,
      sizeof(be_key),
      payload,
      payload_offset,
      sizeof(PAYLOAD));
  });
}

ErrorCode MasstreeStorage::insert_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  return masstree_retry([this, context, key, key_length, payload, payload_count]() {
    MasstreeBorderPage* border;
    uint8_t index;
    xct::XctId observed;
    CHECK_ERROR_CODE(pimpl_->reserve_record(
      context,
      key,
      key_length,
      payload_count,
      &border,
      &index,
      &observed));
    return pimpl_->insert_general(
      context,
      border,
      index,
      observed,
      key,
      key_length,
      payload,
      payload_count);
  });
}

ErrorCode MasstreeStorage::insert_record_normalized(
  thread::Thread* context,
  KeySlice key,
  const void* payload,
  uint16_t payload_count) {
  return masstree_retry([this, context, key, payload, payload_count]() {
    MasstreeBorderPage* border;
    uint8_t index;
    xct::XctId observed;
    CHECK_ERROR_CODE(pimpl_->reserve_record_normalized(
      context,
      key,
      payload_count,
      &border,
      &index,
      &observed));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->insert_general(
      context,
      border,
      index,
      observed,
      &be_key,
      sizeof(be_key),
      payload,
      payload_count);
  });
}

ErrorCode MasstreeStorage::delete_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length) {
  return masstree_retry([this, context, key, key_length]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record(context, key, key_length, true, &border, &index));
    return pimpl_->delete_general(context, border, index, key, key_length);
  });
}

ErrorCode MasstreeStorage::delete_record_normalized(
  thread::Thread* context,
  KeySlice key) {
  return masstree_retry([this, context, key]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record_normalized(context, key, true, &border, &index));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->delete_general(context, border, index, &be_key, sizeof(be_key));
  });
}

ErrorCode MasstreeStorage::overwrite_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return masstree_retry([this, context, key, key_length, payload, payload_offset, payload_count]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record(context, key, key_length, true, &border, &index));
    return pimpl_->overwrite_general(
      context,
      border,
      index,
      key,
      key_length,
      payload,
      payload_offset,
      payload_count);
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::overwrite_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD payload,
  uint16_t payload_offset) {
  return masstree_retry([this, context, key, key_length, payload, payload_offset]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record(context, key, key_length, true, &border, &index));
    return pimpl_->overwrite_general(
      context,
      border,
      index,
      key,
      key_length,
      &payload,
      payload_offset,
      sizeof(payload));
  });
}

ErrorCode MasstreeStorage::overwrite_record_normalized(
  thread::Thread* context,
  KeySlice key,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return masstree_retry([this, context, key, payload, payload_offset, payload_count]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record_normalized(context, key, true, &border, &index));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->overwrite_general(
      context,
      border,
      index,
      &be_key,
      sizeof(be_key),
      payload,
      payload_offset,
      payload_count);
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::overwrite_record_primitive_normalized(
  thread::Thread* context,
  KeySlice key,
  PAYLOAD payload,
  uint16_t payload_offset) {
  return masstree_retry([this, context, key, payload, payload_offset]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record_normalized(context, key, true, &border, &index));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->overwrite_general(
      context,
      border,
      index,
      &be_key,
      sizeof(be_key),
      &payload,
      payload_offset,
      sizeof(payload));
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::increment_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* value,
  uint16_t payload_offset) {
  return masstree_retry([this, context, key, key_length, value, payload_offset]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record(context, key, key_length, true, &border, &index));
    return pimpl_->increment_general<PAYLOAD>(
      context,
      border,
      index,
      key,
      key_length,
      value,
      payload_offset);
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::increment_record_normalized(
  thread::Thread* context,
  KeySlice key,
  PAYLOAD* value,
  uint16_t payload_offset) {
  return masstree_retry([this, context, key, value, payload_offset]() {
    MasstreeBorderPage* border;
    uint8_t index;
    CHECK_ERROR_CODE(pimpl_->locate_record_normalized(context, key, true, &border, &index));
    uint64_t be_key = assorted::htobe<uint64_t>(key);
    return pimpl_->increment_general<PAYLOAD>(
      context,
      border,
      index,
      &be_key,
      sizeof(be_key),
      value,
      payload_offset);
  });
}

ErrorStack MasstreeStorage::verify_single_thread(thread::Thread* context) {
  return pimpl_->verify_single_thread(context);
}


// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_1(x) template ErrorCode MasstreeStorage::get_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* payload, \
    uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_1);

#define EXPIN_2(x) template ErrorCode \
  MasstreeStorage::get_record_primitive_normalized< x > \
  (thread::Thread* context, KeySlice key, x* payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_2);

#define EXPIN_3(x) template ErrorCode MasstreeStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_3);

#define EXPIN_4(x) template ErrorCode \
  MasstreeStorage::overwrite_record_primitive_normalized< x > \
  (thread::Thread* context, KeySlice key, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_4);

#define EXPIN_5(x) template ErrorCode MasstreeStorage::increment_record< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);

#define EXPIN_6(x) template ErrorCode MasstreeStorage::increment_record_normalized< x > \
  (thread::Thread* context, KeySlice key, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_6);
// @endcond

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
