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
#ifndef FOEDUS_STORAGE_HASH_HASH_STORAGE_HPP_
#define FOEDUS_STORAGE_HASH_HASH_STORAGE_HPP_
#include <iosfwd>
#include <string>

#include "foedus/attachable.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace storage {
namespace hash {
/**
 * @brief Represents a key-value store based on a dense and regular hash.
 * @ingroup HASH
 */
class HashStorage CXX11_FINAL : public Storage<HashStorageControlBlock> {
 public:
  typedef HashStoragePimpl   ThisPimpl;
  typedef HashCreateLogType  ThisCreateLogType;
  typedef HashMetadata       ThisMetadata;

  HashStorage();
  HashStorage(Engine* engine, HashStorageControlBlock* control_block);
  HashStorage(Engine* engine, StorageControlBlock* control_block);
  HashStorage(Engine* engine, StorageId id);
  HashStorage(Engine* engine, const StorageName& name);
  HashStorage(const HashStorage& other);
  HashStorage& operator=(const HashStorage& other);

  // Storage interface
  const HashMetadata* get_hash_metadata()  const;
  ErrorStack          create(const Metadata &metadata);
  ErrorStack          load(const StorageControlBlock& snapshot_block);
  ErrorStack          drop();
  friend std::ostream& operator<<(std::ostream& o, const HashStorage& v);

  // this storage type doesn't use moved bit...so far.

  //// Hash table API
  // TODO(Hideaki) Add primitive-optimized versions and increment versions. Later.

  // get_record() methods

  /**
   * @brief Retrieves an entire payload of the given key in this hash storage.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @param[out] payload Buffer to receive the payload of the record.
   * @param[in,out] payload_capacity [In] Byte size of the payload buffer, [Out] length of
   * the payload. This is set whether the payload capacity was too small or not.
   * @details
   * When payload_capacity is smaller than the actual payload, this method returns
   * kErrorCodeStrTooSmallPayloadBuffer and payload_capacity is set to be the required length.
   *
   * When the key is not found (kErrorCodeStrKeyNotFound), we add an appropriate
   * bin mod counter to read set because it is part of a transactional information.
   */
  ErrorCode get_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    void* payload,
    uint16_t* payload_capacity);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY>
  inline ErrorCode get_record(
    thread::Thread* context,
    KEY key,
    void* payload,
    uint16_t* payload_capacity) {
    return get_record(context, &key, sizeof(key), payload, payload_capacity);
  }

  /** If you have already computed HashCombo, use this. */
  ErrorCode get_record(
    thread::Thread* context,
    const HashCombo& combo,
    void* payload,
    uint16_t* payload_capacity);

  /**
   * @brief Retrieves a part of the given key in this hash storage.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @param[out] payload Buffer to receive the payload of the record.
   * @param[in] payload_offset We copy from this byte position of the record.
   * @param[in] payload_count How many bytes we copy.
   * @pre payload_offset + payload_count must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   */
  ErrorCode get_record_part(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY>
  inline ErrorCode get_record_part(
    thread::Thread* context,
    KEY key,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count) {
    return get_record_part(context, &key, sizeof(key), payload, payload_offset, payload_count);
  }

  /** If you have already computed HashCombo, use this. */
  ErrorCode get_record_part(
    thread::Thread* context,
    const HashCombo& combo,
    void* payload,
    uint16_t payload_offset,
    uint16_t* payload_capacity);

  /**
   * @brief Retrieves a part of the given key in this storage as a primitive value.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @param[out] payload Receive the payload of the record.
   * @param[in] payload_offset We copy from this byte position of the record.
   * @pre payload_offset + sizeof(PAYLOAD) must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   * @tparam PAYLOAD primitive type of the payload. all integers and floats are allowed.
   */
  template <typename PAYLOAD>
  ErrorCode   get_record_primitive(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    PAYLOAD* payload,
    uint16_t payload_offset);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY, typename PAYLOAD>
  inline ErrorCode get_record_primitive(
    thread::Thread* context,
    KEY key,
    PAYLOAD* payload,
    uint16_t payload_offset) {
    return get_record_primitive<PAYLOAD>(context, &key, sizeof(key), payload, payload_offset);
  }

  /** If you have already computed HashCombo, use this. */
  template <typename PAYLOAD>
  ErrorCode   get_record_primitive(
    thread::Thread* context,
    const HashCombo& combo,
    PAYLOAD* payload,
    uint16_t payload_offset);

  // insert_record() methods

  /**
   * @brief Inserts a new record of the given key in this hash storage.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @param[in] payload Value to insert.
   * @param[in] payload_count Length of payload.
   * @details
   * If the key already exists, it returns kErrorCodeStrKeyAlreadyExists and we add an appropriate
   * bin mod counter to read set because it is part of a transactional information.
   */
  ErrorCode   insert_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_count);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY>
  inline ErrorCode insert_record(
    thread::Thread* context,
    KEY key,
    const void* payload,
    uint16_t payload_count) {
    return insert_record(context, &key, sizeof(key), payload, payload_count);
  }

  /** If you have already computed HashCombo, use this. */
  ErrorCode   insert_record(
    thread::Thread* context,
    const HashCombo& combo,
    const void* payload,
    uint16_t payload_count);

  // delete_record() methods

  /**
   * @brief Deletes a record of the given key from this hash storage.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @details
   * When the key does not exist, it returns kErrorCodeStrKeyNotFound and we add an appropriate
   * bin mod counter to read set because it is part of a transactional information.
   */
  ErrorCode   delete_record(thread::Thread* context, const void* key, uint16_t key_length);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY>
  inline ErrorCode delete_record(thread::Thread* context, KEY key) {
    return delete_record(context, &key, sizeof(key));
  }

  /** If you have already computed HashCombo, use this. */
  ErrorCode   delete_record(thread::Thread* context, const HashCombo& combo);

  // overwrite_record() methods

  /**
   * @brief Overwrites a part of one record of the given key in this hash storage.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @param[in] payload We copy from this buffer. Must be at least payload_count.
   * @param[in] payload_offset We overwrite to this byte position of the record.
   * @param[in] payload_count How many bytes we overwrite.
   * @details
   * When payload_offset+payload_count is larger than the actual payload, this method returns
   * kErrorCodeStrTooShortPayload. Just like others, when the key does not exist,
   * it returns kErrorCodeStrKeyNotFound and we add an appropriate
   * bin mod counter to read set because it is part of a transactional information.
   */
  ErrorCode   overwrite_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY>
  inline ErrorCode overwrite_record(
    thread::Thread* context,
    KEY key,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count) {
    return overwrite_record(context, &key, sizeof(key), payload, payload_offset, payload_count);
  }

  /** If you have already computed HashCombo, use this. */
  ErrorCode   overwrite_record(
    thread::Thread* context,
    const HashCombo& combo,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /**
   * @brief Overwrites a part of one record of the given key in this storage as a primitive value.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @param[in] payload We copy this value.
   * @param[in] payload_offset We overwrite to this byte position of the record.
   * @pre payload_offset + sizeof(PAYLOAD) must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   * @tparam PAYLOAD primitive type of the payload. all integers and floats are allowed.
   */
  template <typename PAYLOAD>
  ErrorCode   overwrite_record_primitive(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    PAYLOAD payload,
    uint16_t payload_offset);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY, typename PAYLOAD>
  inline ErrorCode overwrite_record_primitive(
    thread::Thread* context,
    KEY key,
    PAYLOAD payload,
    uint16_t payload_offset) {
    return overwrite_record_primitive(context, &key, sizeof(key), payload, payload_offset);
  }

  /** If you have already computed HashCombo, use this. */
  template <typename PAYLOAD>
  ErrorCode   overwrite_record_primitive(
    thread::Thread* context,
    const HashCombo& combo,
    PAYLOAD payload,
    uint16_t payload_offset);

  // increment_record() methods

  /**
   * @brief This one further optimizes overwrite methods for the frequent use
   * case of incrementing some data in primitive type.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key.
   * @param[in] key_length Byte size of key.
   * @param[in,out] value (in) addendum, (out) value after addition.
   * @param[in] payload_offset We overwrite to this byte position of the record.
   * @pre payload_offset + sizeof(PAYLOAD) must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   * @tparam PAYLOAD primitive type of the payload. all integers and floats are allowed.
   */
  template <typename PAYLOAD>
  ErrorCode   increment_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    PAYLOAD* value,
    uint16_t payload_offset);

  /** Overlord to receive key as a primitive type. */
  template <typename KEY, typename PAYLOAD>
  inline ErrorCode increment_record(
    thread::Thread* context,
    KEY key,
    PAYLOAD* value,
    uint16_t payload_offset) {
    return increment_record(context, &key, sizeof(key), value, payload_offset);
  }

  /** If you have already computed HashCombo, use this. */
  template <typename PAYLOAD>
  ErrorCode   increment_record(
    thread::Thread* context,
    const HashCombo& combo,
    PAYLOAD* value,
    uint16_t payload_offset);
};
}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_STORAGE_HPP_
