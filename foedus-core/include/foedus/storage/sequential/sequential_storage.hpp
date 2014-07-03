/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/**
 * @brief Represents an append/scan-only store.
 * @ingroup SEQUENTIAL
 */
class SequentialStorage CXX11_FINAL : public virtual Storage {
 public:
  /**
   * Constructs an sequential storage either from disk or newly create.
   * @param[in] engine Database engine
   * @param[in] metadata Metadata of this storage
   * @param[in] create If true, we newly allocate this sequential when create() is called.
   */
  SequentialStorage(Engine* engine, const SequentialMetadata &metadata, bool create);
  ~SequentialStorage() CXX11_OVERRIDE;

  // Disable default constructors
  SequentialStorage() CXX11_FUNC_DELETE;
  SequentialStorage(const SequentialStorage&) CXX11_FUNC_DELETE;
  SequentialStorage& operator=(const SequentialStorage&) CXX11_FUNC_DELETE;

  // Initializable interface
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  // Storage interface
  StorageId           get_id()    const CXX11_OVERRIDE;
  StorageType         get_type()  const CXX11_OVERRIDE { return kSequentialStorage; }
  const std::string&  get_name()  const CXX11_OVERRIDE;
  const Metadata*     get_metadata()  const CXX11_OVERRIDE;
  const SequentialMetadata*  get_sequential_metadata()  const;
  bool                exists()    const CXX11_OVERRIDE;
  ErrorStack          create(thread::Thread* context) CXX11_OVERRIDE;

  /**
   * @brief Append one record to this sequential storage.
   * @param[in] context Thread context
   * @param[in] payload We copy from this buffer. Must be at least get_payload_size().
   * @param[in] payload_count Length of payload.
   * @pre payload_count > 0
   * @pre payload_count < kMaxPayload
   * @details
   * The strict ordering of the appended record is NOT guaranteed to be the commit serialization
   * order. \ref SEQUENTIAL storage essentially provides a set semantics, not a strictly
   * ordered LIFO queue, to be more scalable. However, the orders are at least loosely
   * ordered; the order largely represents when it was inserted.
   */
  ErrorCode  append_record(thread::Thread* context, const void *payload, uint16_t payload_count);

  void       describe(std::ostream* o) const CXX11_OVERRIDE;

  /** Use this only if you know what you are doing. */
  SequentialStoragePimpl*  get_pimpl() { return pimpl_; }

 private:
  SequentialStoragePimpl*  pimpl_;
};

/**
 * @brief Factory object for sequential storages.
 * @ingroup SEQUENTIAL
 */
class SequentialStorageFactory CXX11_FINAL : public virtual StorageFactory {
 public:
  ~SequentialStorageFactory() {}
  StorageType   get_type() const CXX11_OVERRIDE { return kSequentialStorage; }
  bool          is_right_metadata(const Metadata *metadata) const;
  ErrorStack    get_instance(Engine* engine, const Metadata *metadata, Storage** storage) const;
  void          add_create_log(const Metadata* metadata, thread::Thread* context) const;
};

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_
