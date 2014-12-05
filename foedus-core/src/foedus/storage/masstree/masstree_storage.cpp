/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_retry_impl.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

MasstreeStorage::MasstreeStorage() : Storage<MasstreeStorageControlBlock>() {}
MasstreeStorage::MasstreeStorage(Engine* engine, MasstreeStorageControlBlock* control_block)
  : Storage<MasstreeStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kMasstreeStorage || !exists());
}
MasstreeStorage::MasstreeStorage(Engine* engine, StorageControlBlock* control_block)
  : Storage<MasstreeStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kMasstreeStorage || !exists());
}
MasstreeStorage::MasstreeStorage(Engine* engine, StorageId id)
  : Storage<MasstreeStorageControlBlock>(engine, id) {}
MasstreeStorage::MasstreeStorage(Engine* engine, const StorageName& name)
  : Storage<MasstreeStorageControlBlock>(engine, name) {}
MasstreeStorage::MasstreeStorage(const MasstreeStorage& other)
  : Storage<MasstreeStorageControlBlock>(other.engine_, other.control_block_) {
}
MasstreeStorage& MasstreeStorage::operator=(const MasstreeStorage& other) {
  engine_ = other.engine_;
  control_block_ = other.control_block_;
  return *this;
}

const MasstreeMetadata* MasstreeStorage::get_masstree_metadata() const  {
  return &control_block_->meta_;
}

ErrorStack  MasstreeStorage::create(const Metadata &metadata) {
  return MasstreeStoragePimpl(this).create(static_cast<const MasstreeMetadata&>(metadata));
}
ErrorStack MasstreeStorage::load(const StorageControlBlock& snapshot_block) {
  return MasstreeStoragePimpl(this).load(snapshot_block);
}
ErrorStack  MasstreeStorage::drop()   { return MasstreeStoragePimpl(this).drop(); }

std::ostream& operator<<(std::ostream& o, const MasstreeStorage& v) {
  o << "<MasstreeStorage>"
    << "<id>" << v.get_id() << "</id>"
    << "<name>" << v.get_name() << "</name>"
    << "</MasstreeStorage>";
  return o;
}

ErrorCode MasstreeStorage::get_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t* payload_capacity) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record(
    context,
    key,
    key_length,
    false,
    &border,
    &index,
    &observed));
  return pimpl.retrieve_general(
    context,
    border,
    index,
    observed,
    payload,
    payload_capacity);
}

ErrorCode MasstreeStorage::get_record_part(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record(
    context,
    key,
    key_length,
    false,
    &border,
    &index,
    &observed));
  return pimpl.retrieve_part_general(
    context,
    border,
    index,
    observed,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::get_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record(
    context,
    key,
    key_length,
    false,
    &border,
    &index,
    &observed));
  return pimpl.retrieve_part_general(
    context,
    border,
    index,
    observed,
    payload,
    payload_offset,
    sizeof(PAYLOAD));
}

ErrorCode MasstreeStorage::get_record_normalized(
  thread::Thread* context,
  KeySlice key,
  void* payload,
  uint16_t* payload_capacity) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record_normalized(
    context,
    key,
    false,
    &border,
    &index,
    &observed));
  return pimpl.retrieve_general(
    context,
    border,
    index,
    observed,
    payload,
    payload_capacity);
}

ErrorCode MasstreeStorage::get_record_part_normalized(
  thread::Thread* context,
  KeySlice key,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record_normalized(
    context,
    key,
    false,
    &border,
    &index,
    &observed));
  return pimpl.retrieve_part_general(
    context,
    border,
    index,
    observed,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::get_record_primitive_normalized(
  thread::Thread* context,
  KeySlice key,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record_normalized(
    context,
    key,
    false,
    &border,
    &index,
    &observed));
  return pimpl.retrieve_part_general(
    context,
    border,
    index,
    observed,
    payload,
    payload_offset,
    sizeof(PAYLOAD));
}

ErrorCode MasstreeStorage::insert_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.reserve_record(
    context,
    key,
    key_length,
    payload_count,
    &border,
    &index,
    &observed));
  return pimpl.insert_general(
    context,
    border,
    index,
    observed,
    key,
    key_length,
    payload,
    payload_count);
}

ErrorCode MasstreeStorage::insert_record_normalized(
  thread::Thread* context,
  KeySlice key,
  const void* payload,
  uint16_t payload_count) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.reserve_record_normalized(
    context,
    key,
    payload_count,
    &border,
    &index,
    &observed));
  uint64_t be_key = assorted::htobe<uint64_t>(key);
  return pimpl.insert_general(
    context,
    border,
    index,
    observed,
    &be_key,
    sizeof(be_key),
    payload,
    payload_count);
}

ErrorCode MasstreeStorage::delete_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record(
    context,
    key,
    key_length,
    true,
    &border,
    &index,
    &observed));
  return pimpl.delete_general(context, border, index, observed, key, key_length);
}

ErrorCode MasstreeStorage::delete_record_normalized(
  thread::Thread* context,
  KeySlice key) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record_normalized(
    context,
    key,
    true,
    &border,
    &index,
    &observed));
  uint64_t be_key = assorted::htobe<uint64_t>(key);
  return pimpl.delete_general(
    context, border, index, observed, &be_key, sizeof(be_key));
}

ErrorCode MasstreeStorage::overwrite_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record(
    context,
    key,
    key_length,
    true,
    &border,
    &index,
    &observed));
  return pimpl.overwrite_general(
    context,
    border,
    index,
    observed,
    key,
    key_length,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::overwrite_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD payload,
  uint16_t payload_offset) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record(
    context,
    key,
    key_length,
    true,
    &border,
    &index,
    &observed));
  return pimpl.overwrite_general(
    context,
    border,
    index,
    observed,
    key,
    key_length,
    &payload,
    payload_offset,
    sizeof(payload));
}

ErrorCode MasstreeStorage::overwrite_record_normalized(
  thread::Thread* context,
  KeySlice key,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record_normalized(
    context,
    key,
    true,
    &border,
    &index,
    &observed));
  uint64_t be_key = assorted::htobe<uint64_t>(key);
  return pimpl.overwrite_general(
    context,
    border,
    index,
    observed,
    &be_key,
    sizeof(be_key),
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::overwrite_record_primitive_normalized(
  thread::Thread* context,
  KeySlice key,
  PAYLOAD payload,
  uint16_t payload_offset) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record_normalized(
    context,
    key,
    true,
    &border,
    &index,
    &observed));
  uint64_t be_key = assorted::htobe<uint64_t>(key);
  return pimpl.overwrite_general(
    context,
    border,
    index,
    observed,
    &be_key,
    sizeof(be_key),
    &payload,
    payload_offset,
    sizeof(payload));
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::increment_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* value,
  uint16_t payload_offset) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record(
    context,
    key,
    key_length,
    true,
    &border,
    &index,
    &observed));
  return pimpl.increment_general<PAYLOAD>(
    context,
    border,
    index,
    observed,
    key,
    key_length,
    value,
    payload_offset);
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::increment_record_normalized(
  thread::Thread* context,
  KeySlice key,
  PAYLOAD* value,
  uint16_t payload_offset) {
  MasstreeBorderPage* border;
  uint8_t index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.locate_record_normalized(
    context,
    key,
    true,
    &border,
    &index,
    &observed));
  uint64_t be_key = assorted::htobe<uint64_t>(key);
  return pimpl.increment_general<PAYLOAD>(
    context,
    border,
    index,
    observed,
    &be_key,
    sizeof(be_key),
    value,
    payload_offset);
}

ErrorStack MasstreeStorage::replace_pointers(const Composer::ReplacePointersArguments& args) {
  return MasstreeStoragePimpl(this).replace_pointers(args);
}

ErrorStack MasstreeStorage::verify_single_thread(thread::Thread* context) {
  return MasstreeStoragePimpl(this).verify_single_thread(context);
}

ErrorStack MasstreeStorage::debugout_single_thread(
  Engine* engine,
  bool volatile_only,
  uint32_t max_pages) {
  return MasstreeStoragePimpl(this).debugout_single_thread(engine, volatile_only, max_pages);
}


ErrorCode MasstreeStorage::prefetch_pages_normalized(
  thread::Thread* context,
  bool install_volatile,
  bool cache_snapshot,
  KeySlice from,
  KeySlice to) {
  return MasstreeStoragePimpl(this).prefetch_pages_normalized(
    context,
    install_volatile,
    cache_snapshot,
    from,
    to);
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
