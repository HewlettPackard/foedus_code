/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
  KeyLength key_length,
  void* payload,
  PayloadLength* payload_capacity,
  bool read_only) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return get_record_normalized(context, slice, payload, payload_capacity, read_only);
  }

  MasstreeBorderPage* border;
  SlotIndex index;
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
    payload_capacity,
    read_only);
}

ErrorCode MasstreeStorage::get_record_part(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  void* payload,
  PayloadLength payload_offset,
  PayloadLength payload_count,
  bool read_only) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return get_record_part_normalized(
      context, slice, payload, payload_offset, payload_count, read_only);
  }

  MasstreeBorderPage* border;
  SlotIndex index;
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
    payload_count,
    read_only);
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::get_record_primitive(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  PAYLOAD* payload,
  PayloadLength payload_offset,
  bool read_only) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return get_record_primitive_normalized<PAYLOAD>(
      context, slice, payload, payload_offset, read_only);
  }

  MasstreeBorderPage* border;
  SlotIndex index;
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
    sizeof(PAYLOAD),
    read_only);
}

ErrorCode MasstreeStorage::get_record_normalized(
  thread::Thread* context,
  KeySlice key,
  void* payload,
  PayloadLength* payload_capacity,
  bool read_only) {
  MasstreeBorderPage* border;
  SlotIndex index;
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
    payload_capacity,
    read_only);
}

ErrorCode MasstreeStorage::get_record_part_normalized(
  thread::Thread* context,
  KeySlice key,
  void* payload,
  PayloadLength payload_offset,
  PayloadLength payload_count,
  bool read_only) {
  MasstreeBorderPage* border;
  SlotIndex index;
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
    payload_count,
    read_only);
}

template <typename PAYLOAD>
ErrorCode MasstreeStorage::get_record_primitive_normalized(
  thread::Thread* context,
  KeySlice key,
  PAYLOAD* payload,
  PayloadLength payload_offset,
  bool read_only) {
  MasstreeBorderPage* border;
  SlotIndex index;
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
    sizeof(PAYLOAD),
    read_only);
}

PayloadLength adjust_payload_hint(
  PayloadLength payload_count,
  PayloadLength physical_payload_hint) {
  ASSERT_ND(physical_payload_hint >= payload_count);  // if not, most likely misuse.
  if (physical_payload_hint < payload_count) {
    physical_payload_hint = payload_count;
  }
  if (physical_payload_hint > kMaxPayloadLength) {
    physical_payload_hint = kMaxPayloadLength;
  }
  physical_payload_hint = assorted::align8(physical_payload_hint);
  return physical_payload_hint;
}

ErrorCode MasstreeStorage::insert_record(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  const void* payload,
  PayloadLength payload_count,
  PayloadLength physical_payload_hint) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return insert_record_normalized(context, slice, payload, payload_count, physical_payload_hint);
  }

  if (UNLIKELY(payload_count > kMaxPayloadLength)) {
    return kErrorCodeStrTooLongPayload;
  }
  physical_payload_hint = adjust_payload_hint(payload_count, physical_payload_hint);
  MasstreeBorderPage* border;
  SlotIndex index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.reserve_record(
    context,
    key,
    key_length,
    payload_count,
    physical_payload_hint,
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
  PayloadLength payload_count,
  PayloadLength physical_payload_hint) {
  if (UNLIKELY(payload_count > kMaxPayloadLength)) {
    return kErrorCodeStrTooLongPayload;
  }
  physical_payload_hint = adjust_payload_hint(payload_count, physical_payload_hint);
  MasstreeBorderPage* border;
  SlotIndex index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.reserve_record_normalized(
    context,
    key,
    payload_count,
    physical_payload_hint,
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
  KeyLength key_length) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return delete_record_normalized(context, slice);
  }

  MasstreeBorderPage* border;
  SlotIndex index;
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
  SlotIndex index;
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

ErrorCode MasstreeStorage::upsert_record(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  const void* payload,
  PayloadLength payload_count,
  PayloadLength physical_payload_hint) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return upsert_record_normalized(context, slice, payload, payload_count, physical_payload_hint);
  }

  if (UNLIKELY(payload_count > kMaxPayloadLength)) {
    return kErrorCodeStrTooLongPayload;
  }
  physical_payload_hint = adjust_payload_hint(payload_count, physical_payload_hint);
  MasstreeBorderPage* border;
  SlotIndex index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.reserve_record(
    context,
    key,
    key_length,
    payload_count,
    physical_payload_hint,
    &border,
    &index,
    &observed));
  return pimpl.upsert_general(
    context,
    border,
    index,
    observed,
    key,
    key_length,
    payload,
    payload_count);
}

ErrorCode MasstreeStorage::upsert_record_normalized(
  thread::Thread* context,
  KeySlice key,
  const void* payload,
  PayloadLength payload_count,
  PayloadLength physical_payload_hint) {
  if (UNLIKELY(payload_count > kMaxPayloadLength)) {
    return kErrorCodeStrTooLongPayload;
  }
  physical_payload_hint = adjust_payload_hint(payload_count, physical_payload_hint);
  MasstreeBorderPage* border;
  SlotIndex index;
  xct::XctId observed;
  MasstreeStoragePimpl pimpl(this);
  CHECK_ERROR_CODE(pimpl.reserve_record_normalized(
    context,
    key,
    payload_count,
    physical_payload_hint,
    &border,
    &index,
    &observed));
  uint64_t be_key = assorted::htobe<uint64_t>(key);
  return pimpl.upsert_general(
    context,
    border,
    index,
    observed,
    &be_key,
    sizeof(be_key),
    payload,
    payload_count);
}

ErrorCode MasstreeStorage::overwrite_record(
  thread::Thread* context,
  const void* key,
  KeyLength key_length,
  const void* payload,
  PayloadLength payload_offset,
  PayloadLength payload_count) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return overwrite_record_normalized(context, slice, payload, payload_offset, payload_count);
  }

  MasstreeBorderPage* border;
  SlotIndex index;
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
  KeyLength key_length,
  PAYLOAD payload,
  PayloadLength payload_offset) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return overwrite_record_primitive_normalized<PAYLOAD>(context, slice, payload, payload_offset);
  }

  MasstreeBorderPage* border;
  SlotIndex index;
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
  PayloadLength payload_offset,
  PayloadLength payload_count) {
  MasstreeBorderPage* border;
  SlotIndex index;
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
  PayloadLength payload_offset) {
  MasstreeBorderPage* border;
  SlotIndex index;
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
  KeyLength key_length,
  PAYLOAD* value,
  PayloadLength payload_offset) {
  // Automatically switch to faster implementation for 8-byte keys
  if (key_length == sizeof(KeySlice)) {
    KeySlice slice = normalize_be_bytes_full(key);
    return increment_record_normalized<PAYLOAD>(context, slice, value, payload_offset);
  }

  MasstreeBorderPage* border;
  SlotIndex index;
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
  PayloadLength payload_offset) {
  MasstreeBorderPage* border;
  SlotIndex index;
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

ErrorStack MasstreeStorage::verify_single_thread(thread::Thread* context) {
  return MasstreeStoragePimpl(this).verify_single_thread(context);
}

ErrorStack MasstreeStorage::debugout_single_thread(
  Engine* engine,
  bool volatile_only,
  uint32_t max_pages) {
  return MasstreeStoragePimpl(this).debugout_single_thread(engine, volatile_only, max_pages);
}

ErrorStack MasstreeStorage::hcc_reset_all_temperature_stat(Engine* engine) {
  return MasstreeStoragePimpl(this).hcc_reset_all_temperature_stat(engine);
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

ErrorStack MasstreeStorage::fatify_first_root(thread::Thread* context, uint32_t desired_count) {
  return MasstreeStoragePimpl(this).fatify_first_root(context, desired_count);
}

SlotIndex MasstreeStorage::estimate_records_per_page(
  Layer layer,
  KeyLength key_length,
  PayloadLength payload_length) {
  PayloadLength aligned_payload = assorted::align8(payload_length);
  KeyLength aligned_suffix = 0;
  if (key_length > (layer + 1U) * sizeof(KeySlice)) {
    aligned_suffix = assorted::align8(key_length - (layer + 1U) * sizeof(KeySlice));
  }
  SlotIndex ret = kBorderPageDataPartSize
    / (aligned_suffix + aligned_payload + kBorderPageSlotSize);
  ASSERT_ND(ret <= kBorderPageMaxSlots);
  return ret;
}

// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_1(x) template ErrorCode MasstreeStorage::get_record_primitive< x > \
  (thread::Thread* context, const void* key, KeyLength key_length, x* payload, \
    PayloadLength payload_offset, bool read_only)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_1);

#define EXPIN_2(x) template ErrorCode \
  MasstreeStorage::get_record_primitive_normalized< x > \
  (thread::Thread* context, KeySlice key, x* payload, PayloadLength payload_offset, bool read_only)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_2);

#define EXPIN_3(x) template ErrorCode MasstreeStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, const void* key, KeyLength key_length, x payload, \
  PayloadLength payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_3);

#define EXPIN_4(x) template ErrorCode \
  MasstreeStorage::overwrite_record_primitive_normalized< x > \
  (thread::Thread* context, KeySlice key, x payload, PayloadLength payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_4);

#define EXPIN_5(x) template ErrorCode MasstreeStorage::increment_record< x > \
  (thread::Thread* context, const void* key, KeyLength key_length, x* value, \
  PayloadLength payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);

#define EXPIN_6(x) template ErrorCode MasstreeStorage::increment_record_normalized< x > \
  (thread::Thread* context, KeySlice key, x* value, PayloadLength payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_6);
// @endcond

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
