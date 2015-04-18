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
#include "foedus/storage/hash/hash_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_combo.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {

HashStorage::HashStorage() : Storage<HashStorageControlBlock>() {}
HashStorage::HashStorage(Engine* engine, HashStorageControlBlock* control_block)
  : Storage<HashStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kHashStorage || !exists());
}
HashStorage::HashStorage(Engine* engine, StorageControlBlock* control_block)
  : Storage<HashStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kHashStorage || !exists());
}
HashStorage::HashStorage(Engine* engine, StorageId id)
  : Storage<HashStorageControlBlock>(engine, id) {}
HashStorage::HashStorage(Engine* engine, const StorageName& name)
  : Storage<HashStorageControlBlock>(engine, name) {}
HashStorage::HashStorage(const HashStorage& other)
  : Storage<HashStorageControlBlock>(other.engine_, other.control_block_) {
}
HashStorage& HashStorage::operator=(const HashStorage& other) {
  engine_ = other.engine_;
  control_block_ = other.control_block_;
  return *this;
}


ErrorStack  HashStorage::create(const Metadata &metadata) {
  return HashStoragePimpl(this).create(static_cast<const HashMetadata&>(metadata));
}
ErrorStack HashStorage::load(const StorageControlBlock& snapshot_block) {
  return HashStoragePimpl(this).load(snapshot_block);
}
ErrorStack  HashStorage::drop() { return HashStoragePimpl(this).drop(); }

const HashMetadata* HashStorage::get_hash_metadata() const  { return &control_block_->meta_; }

ErrorCode HashStorage::get_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t* payload_capacity) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).get_record(context, combo, payload, payload_capacity);
}

ErrorCode HashStorage::get_record_part(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).get_record_part(
    context,
    combo,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode HashStorage::get_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).get_record_primitive(context, combo, payload, payload_offset);
}

ErrorCode HashStorage::insert_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).insert_record(context, combo, payload, payload_count);
}

ErrorCode HashStorage::delete_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).delete_record(context, combo);
}

ErrorCode HashStorage::overwrite_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).overwrite_record(
    context,
    combo,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode HashStorage::overwrite_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD payload,
  uint16_t payload_offset) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).overwrite_record_primitive(
    context,
    combo,
    payload,
    payload_offset);
}

template <typename PAYLOAD>
ErrorCode HashStorage::increment_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* value,
  uint16_t payload_offset) {
  HashCombo combo(key, key_length, *get_hash_metadata());
  return HashStoragePimpl(this).increment_record(context, combo, value, payload_offset);
}

std::ostream& operator<<(std::ostream& o, const HashStorage& v) {
  o << "<HashStorage>"
    << "<id>" << v.get_id() << "</id>"
    << "<name>" << v.get_name() << "</name>"
    << "<bin_bits>" << static_cast<int>(v.control_block_->meta_.bin_bits_) << "</bin_bits>"
    << "</HashStorage>";
  return o;
}

// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_2(x) template ErrorCode HashStorage::get_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_2);

#define EXPIN_3(x) template ErrorCode HashStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_3);

#define EXPIN_5(x) template ErrorCode HashStorage::increment_record< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);
// @endcond


}  // namespace hash
}  // namespace storage
}  // namespace foedus
