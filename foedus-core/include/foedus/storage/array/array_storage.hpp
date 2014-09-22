/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_

#include <cstring>
#include <iosfwd>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/attachable.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_route.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace array {

/**
 * @brief Represents a key-value store based on a dense and regular array.
 * @ingroup ARRAY
 */
class ArrayStorage CXX11_FINAL
  : public virtual Storage, public Attachable<ArrayStorageControlBlock> {
 public:
  ArrayStorage() : Attachable<ArrayStorageControlBlock>() {}
  /**
   * Constructs an array storage either from disk or newly create.
   */
  ArrayStorage(Engine* engine, ArrayStorageControlBlock* control_block)
    : Attachable<ArrayStorageControlBlock>(engine, control_block) {
    ASSERT_ND(get_type() == kArrayStorage || !exists());
  }
  ArrayStorage(Engine* engine, StorageControlBlock* control_block)
    : Attachable<ArrayStorageControlBlock>(
      engine,
      reinterpret_cast<ArrayStorageControlBlock*>(control_block)) {
    ASSERT_ND(get_type() == kArrayStorage || !exists());
  }
  ArrayStorage(const ArrayStorage& other)
    : Attachable<ArrayStorageControlBlock>(other.engine_, other.control_block_) {
  }
  ArrayStorage& operator=(const ArrayStorage& other) {
    engine_ = other.engine_;
    control_block_ = other.control_block_;
    return *this;
  }

  // Storage interface
  StorageId           get_id()    const CXX11_OVERRIDE;
  StorageType         get_type()  const CXX11_OVERRIDE;
  const StorageName&  get_name()  const CXX11_OVERRIDE;
  const Metadata*     get_metadata()  const CXX11_OVERRIDE;
  const ArrayMetadata*  get_array_metadata()  const;
  bool                exists()    const CXX11_OVERRIDE;
  ErrorStack          create(const Metadata &metadata) CXX11_OVERRIDE;
  ErrorStack          drop() CXX11_OVERRIDE;

  /**
   * @brief Prefetch data pages in this storage.
   * @param[in] context Thread context.
   * @param[in] from inclusive begin offset of records that are specifically prefetched even in
   * data pages.
   * @param[in] to exclusive end offset of records that are specifically prefetched even in data
   * pages. 0 means up to the end of the storage.
   * @details
   * This is to \e warmup the storage for the current core.
   * Data pages are prefetched within from/to.
   * So far prefetches only volatile pages, but it will also cache and prefetch snapshot pages.
   */
  ErrorCode prefetch_pages(thread::Thread* context, ArrayOffset from = 0, ArrayOffset to = 0);

  // this storage type doesn't use moved bit
  bool track_moved_record(xct::WriteXctAccess* /*write*/) CXX11_OVERRIDE {
    ASSERT_ND(false);
    return false;
  }
  xct::LockableXctId* track_moved_record(xct::LockableXctId* /*address*/) CXX11_OVERRIDE {
    ASSERT_ND(false);
    return CXX11_NULLPTR;
  }

  /**
   * @brief Returns byte size of one record in this array storage without internal overheads.
   * @details
   * ArrayStorage is a fix-sized storage, thus we have this interface in storage level
   * rather than in record level.
   */
  uint16_t    get_payload_size() const;

  /** Returns the size of this array. */
  ArrayOffset get_array_size() const;

  /** Returns the number of levels. */
  uint8_t     get_levels() const;

  /**
   * @brief Retrieves one record of the given offset in this array storage.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[out] payload We copy the record to this address. Must be at least get_payload_size().
   * @pre offset < get_array_size()
   * @details
   * Equivalent to get_record(context, offset, payload, 0, get_payload_size()).
   */
  ErrorCode  get_record(thread::Thread* context, ArrayOffset offset, void *payload);

  /**
   * @brief Retrieves a part of record of the given offset in this array storage.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[out] payload We copy the record to this address. Must be at least payload_count.
   * @param[in] payload_offset We copy from this byte position of the record.
   * @param[in] payload_count How many bytes we copy.
   * @pre payload_offset + payload_count <= get_payload_size()
   * @pre offset < get_array_size()
   */
  ErrorCode  get_record(thread::Thread* context, ArrayOffset offset,
            void *payload, uint16_t payload_offset, uint16_t payload_count);

  /**
   * @brief Retrieves a part of record of the given offset as a primitive type
   * in this array storage. A bit more efficient than get_record().
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[out] payload We copy the record to this address.
   * @param[in] payload_offset We copy from this byte position of the record.
   * @tparam T primitive type. All integers and floats are allowed.
   * @pre payload_offset + sizeof(T) <= get_payload_size()
   * @pre offset < get_array_size()
   */
  template <typename T>
  ErrorCode  get_record_primitive(thread::Thread* context, ArrayOffset offset,
            T *payload, uint16_t payload_offset);

  /**
   * @brief Retrieves a pointer to the entire payload.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[out] payload Sets the pointer to the payload.
   * @pre offset < get_array_size()
   * @details
   * This is used to retrieve entire record without copying.
   * The record is protected by read-set (if there is any change, it will abort at pre-commit).
   * \b However, we might read a half-changed value in the meantime.
   */
  ErrorCode get_record_payload(thread::Thread* context, ArrayOffset offset, const void** payload);

  /**
   * @brief Retrieves a pointer to the entire record for write (thus always in volatile page).
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[out] record Sets the pointer to the record in a volatile page.
   * @pre offset < get_array_size()
   * @details
   * This is used to retrieve entire record without copying and also to prepare for writes.
   * Though "record" is a non-const pointer, of course don't directly write to it.
   * Use overwrite_xxx etc which does the pre-commit protocol to write.
   * This also adds to read-set.
   */
  ErrorCode get_record_for_write(thread::Thread* context, ArrayOffset offset, Record** record);

  /** batched interface */
  template <typename T>
  ErrorCode get_record_primitive_batch(
    thread::Thread* context,
    uint16_t payload_offset,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    T *payload);
  ErrorCode get_record_payload_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    const void** payload_batch);
  ErrorCode get_record_for_write_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    Record** record_batch);

  /**
   * @brief Overwrites one record of the given offset in this array storage.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[in] payload We copy from this buffer. Must be at least get_payload_size().
   * @pre offset < get_array_size()
   * @details
   * Equivalent to overwrite_record(context, offset, payload, 0, get_payload_size()).
   */
  ErrorCode  overwrite_record(thread::Thread* context, ArrayOffset offset, const void *payload) {
    return overwrite_record(context, offset, payload, 0, get_payload_size());
  }
  /**
   * @brief Overwrites a part of one record of the given offset in this array storage.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[in] payload We copy from this buffer. Must be at least get_payload_size().
   * @param[in] payload_offset We copy to this byte position of the record.
   * @param[in] payload_count How many bytes we copy.
   * @pre payload_offset + payload_count <= get_payload_size()
   * @pre offset < get_array_size()
   */
  ErrorCode  overwrite_record(thread::Thread* context, ArrayOffset offset,
            const void *payload, uint16_t payload_offset, uint16_t payload_count);

  /**
   * @brief Overwrites a part of record of the given offset as a primitive type
   * in this array storage. A bit more efficient than overwrite_record().
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[in] payload The value as primitive type.
   * @param[in] payload_offset We copy to this byte position of the record.
   * @tparam T primitive type. All integers and floats are allowed.
   * @pre payload_offset + sizeof(T) <= get_payload_size()
   * @pre offset < get_array_size()
   */
  template <typename T>
  ErrorCode  overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
            T payload, uint16_t payload_offset);

  /**
   * @brief Overwrites a part of the pre-searched record in this array storage.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array. Used for logging.
   * @param[in] record Pointer to the record.
   * @param[in] payload We copy from this buffer.
   * @param[in] payload_offset We copy to this byte position of the record.
   * @param[in] payload_count How many bytes we copy.
   * @pre payload_offset + payload_count <= get_payload_size()
   * @pre offset < get_array_size()
   * @see get_record_for_write()
   */
  ErrorCode  overwrite_record(
    thread::Thread* context,
    ArrayOffset offset,
    Record* record,
    const void *payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /**
   * @brief Overwrites a part of pre-searched record as a primitive type
   * in this array storage. A bit more efficient than overwrite_record().
   * @param[in] context Thread context
   * @param[in] offset The offset in this array. Used for logging.
   * @param[in] record Pointer to the record.
   * @param[in] payload The value as primitive type.
   * @param[in] payload_offset We copy to this byte position of the record.
   * @tparam T primitive type. All integers and floats are allowed.
   * @pre payload_offset + sizeof(T) <= get_payload_size()
   * @pre offset < get_array_size()
   */
  template <typename T>
  ErrorCode  overwrite_record_primitive(
    thread::Thread* context,
    ArrayOffset offset,
    Record* record,
    T payload,
    uint16_t payload_offset);

  /**
   * @brief This one further optimizes overwrite_record_primitive() for the frequent use
   * case of incrementing some data in primitive type.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[in,out] value (in) addendum, (out) value after addition.
   * @param[in] payload_offset We write to this byte position of the record.
   * @tparam T primitive type. All integers and floats are allowed.
   * @pre payload_offset + sizeof(T) <= get_payload_size()
   * @pre offset < get_array_size()
   * @details
   * This method combines get and overwrite, so it can halve the number of tree lookup.
   * This method can be only provided with template, so we omit "_primitive".
   */
  template <typename T>
  ErrorCode  increment_record(thread::Thread* context, ArrayOffset offset,
            T *value, uint16_t payload_offset);

  /**
   * @brief This is a faster increment that does not return the value after increment.
   * @param[in] context Thread context
   * @param[in] offset The offset in this array
   * @param[in] value addendum
   * @param[in] payload_offset We write to this byte position of the record.
   * @tparam T primitive type. All integers and floats are allowed.
   * @pre payload_offset + sizeof(T) <= get_payload_size()
   * @pre offset < get_array_size()
   * @details
   * This method is faster than increment_record because it doesn't rely on the current value.
   * This uses a rare "write-set only" log.
   * other increments have to check deletion bit at least.
   */
  template <typename T>
  ErrorCode  increment_record_oneshot(
    thread::Thread* context,
    ArrayOffset offset,
    T value,
    uint16_t payload_offset);

  void        describe(std::ostream* o) const CXX11_OVERRIDE;

  ErrorStack  verify_single_thread(thread::Thread* context);
};

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
