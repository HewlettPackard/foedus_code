/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_HASH_HASH_STORAGE_HPP_
#define FOEDUS_STORAGE_HASH_HASH_STORAGE_HPP_
#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace hash {
/**
 * @brief Represents a key-value store based on a dense and regular hash.
 * @ingroup HASH
 */
class HashStorage CXX11_FINAL : public virtual Storage {
 public:
  /**
   * Constructs an hash storage either from disk or newly create.
   * @param[in] engine Database engine
   * @param[in] metadata Metadata of this storage
   * @param[in] create If true, we newly allocate this hash when create() is called.
   */
  HashStorage(Engine* engine, const HashMetadata &metadata, bool create);
  ~HashStorage() CXX11_OVERRIDE;

  // Disable default constructors
  HashStorage() CXX11_FUNC_DELETE;
  HashStorage(const HashStorage&) CXX11_FUNC_DELETE;
  HashStorage& operator=(const HashStorage&) CXX11_FUNC_DELETE;

  // Initializable interface
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  // Storage interface
  StorageId           get_id()    const CXX11_OVERRIDE;
  StorageType         get_type()  const CXX11_OVERRIDE { return kHashStorage; }
  const std::string&  get_name()  const CXX11_OVERRIDE;
  const Metadata*     get_metadata()  const CXX11_OVERRIDE;
  const HashMetadata* get_hash_metadata()  const;
  bool                exists()    const CXX11_OVERRIDE;
  ErrorStack          create(thread::Thread* context) CXX11_OVERRIDE;
  void                describe(std::ostream* o) const CXX11_OVERRIDE;


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
   * On the other hand, when the key is not found (kErrorCodeStrKeyNotFound), we add an appropriate
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

  void aaa(thread::Thread* context);
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

  // log apply methods.
  // some of them are so trivial that they are inlined in log class.

  void        apply_insert_record(
    thread::Thread* context,
    const HashInsertLogType* log_entry,
    Record* record);
  void        apply_delete_record(
    thread::Thread* context,
    const HashDeleteLogType* log_entry,
    Record* record);


  /** Use this only if you know what you are doing. */
  HashStoragePimpl*  get_pimpl() { return pimpl_; }

 private:
  HashStoragePimpl*  pimpl_;
};

/**
 * @brief Factory object for hash storages.
 * @ingroup HASH
 */
class HashStorageFactory CXX11_FINAL : public virtual StorageFactory {
 public:
  ~HashStorageFactory() {}
  StorageType   get_type() const CXX11_OVERRIDE { return kHashStorage; }
  bool          is_right_metadata(const Metadata *metadata) const;
  ErrorStack    get_instance(Engine* engine, const Metadata *metadata, Storage** storage) const;
  void          add_create_log(const Metadata* metadata, thread::Thread* context) const;
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_STORAGE_HPP_
