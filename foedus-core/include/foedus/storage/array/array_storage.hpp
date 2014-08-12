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
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
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
 * So far experimental...
 * This caches a small number of variables, most likely on local cacheline.
 * @ingroup ARRAY
 */
struct ArrayStorageCache CXX11_FINAL {
  ArrayStorageCache();
  explicit ArrayStorageCache(ArrayStorage *storage);

  explicit ArrayStorageCache(const ArrayStorageCache& other) { operator=(other); }
  ArrayStorageCache& operator=(const ArrayStorageCache& other) {
    engine_ = other.engine_;
    storage_ = other.storage_;
    pimpl_ = other.pimpl_;
    metadata_ = other.metadata_;
    root_page_ = other.root_page_;
    levels_ = other.levels_;
    route_finder_ = other.route_finder_;
    return *this;
  }
  void assert_initialized() {
    ASSERT_ND(engine_);
    ASSERT_ND(storage_);
    ASSERT_ND(pimpl_);
    ASSERT_ND(root_page_);
    ASSERT_ND(metadata_.id_ != 0);
    ASSERT_ND(route_finder_.get_records_in_leaf() != 0);
  }

  Engine*             engine_;
  ArrayStorage*       storage_;
  ArrayStoragePimpl*  pimpl_;
  ArrayMetadata       metadata_;
  ArrayPage*          root_page_;
  uint8_t             levels_;
  LookupRouteFinder   route_finder_;
};

/**
 * @brief Represents a key-value store based on a dense and regular array.
 * @ingroup ARRAY
 */
class ArrayStorage CXX11_FINAL : public virtual Storage {
 public:
  /**
   * Constructs an array storage either from disk or newly create.
   * @param[in] engine Database engine
   * @param[in] metadata Metadata of this storage
   * @param[in] create If true, we newly allocate this array when create() is called.
   */
  ArrayStorage(Engine* engine, const ArrayMetadata &metadata, bool create);
  ~ArrayStorage() CXX11_OVERRIDE;

  // Disable default constructors
  ArrayStorage() CXX11_FUNC_DELETE;
  ArrayStorage(const ArrayStorage&) CXX11_FUNC_DELETE;
  ArrayStorage& operator=(const ArrayStorage&) CXX11_FUNC_DELETE;

  // Initializable interface
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  // Storage interface
  StorageId           get_id()    const CXX11_OVERRIDE;
  StorageType         get_type()  const CXX11_OVERRIDE { return kArrayStorage; }
  const std::string&  get_name()  const CXX11_OVERRIDE;
  const Metadata*     get_metadata()  const CXX11_OVERRIDE;
  const ArrayMetadata*  get_array_metadata()  const;
  bool                exists()    const CXX11_OVERRIDE;
  ErrorStack          create(thread::Thread* context) CXX11_OVERRIDE;

  // this storage type doesn't use moved bit
  bool track_moved_record(xct::WriteXctAccess* /*write*/) CXX11_OVERRIDE {
    ASSERT_ND(false);
    return false;
  }
  xct::XctId* track_moved_record(xct::XctId* /*address*/) CXX11_OVERRIDE {
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

  /** Use this only if you know what you are doing. */
  ArrayStoragePimpl*  get_pimpl() { return pimpl_; }

  /** So far experimental... */
  static ErrorCode get_record(
    thread::Thread* context,
    const ArrayStorageCache& cache,
    ArrayOffset offset,
    void *payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** So far experimental... */
  template <typename T>
  static ErrorCode get_record_primitive(
    thread::Thread* context,
    const ArrayStorageCache& cache,
    ArrayOffset offset,
    T *payload,
    uint16_t payload_offset);

  static ErrorCode get_record_payload(
    thread::Thread* context,
    const ArrayStorageCache& cache,
    ArrayOffset offset,
    const void** payload);

 private:
  ArrayStoragePimpl*  pimpl_;
};

/**
 * @brief Factory object for array storages.
 * @ingroup ARRAY
 */
class ArrayStorageFactory CXX11_FINAL : public virtual StorageFactory {
 public:
  ~ArrayStorageFactory() {}
  StorageType   get_type() const CXX11_OVERRIDE { return kArrayStorage; }
  bool          is_right_metadata(const Metadata *metadata) const;
  ErrorStack    get_instance(Engine* engine, const Metadata *metadata, Storage** storage) const;
  void          add_create_log(const Metadata* metadata, thread::Thread* context) const;
};

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_HPP_
