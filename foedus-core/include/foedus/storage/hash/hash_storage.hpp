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
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
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
  const HashMetadata*  get_hash_metadata()  const;
  bool                exists()    const CXX11_OVERRIDE;
  ErrorStack          create(thread::Thread* context) CXX11_OVERRIDE;

 
  /**
   * @brief Retrieves one record of the given offset in this hash storage.
   * @param[in] context Thread context
   * @param[in] offset The offset in this hash
   * @param[out] payload We copy the record to this address. Must be at least get_payload_size().
   * @pre offset < get_hash_size()
   * @details
   * Equivalent to get_record(context, offset, payload, 0, get_payload_size()).
   */
  ErrorCode  get_record(thread::Thread* context, const void *key, uint16_t key_length, void *payload, uint16_t payload_offset, uint16_t payload_count);

  void        describe(std::ostream* o) const CXX11_OVERRIDE;

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
