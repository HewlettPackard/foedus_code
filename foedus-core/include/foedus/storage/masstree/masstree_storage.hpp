/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_HPP_

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Represents a Masstree storage.
 * @ingroup MASSTREE
 */
class MasstreeStorage CXX11_FINAL : public virtual Storage {
 public:
  /**
   * Constructs an masstree storage either from disk or newly create.
   * @param[in] engine Database engine
   * @param[in] metadata Metadata of this storage
   * @param[in] create If true, we newly allocate this masstree when create() is called.
   */
  MasstreeStorage(Engine* engine, const MasstreeMetadata &metadata, bool create);
  ~MasstreeStorage() CXX11_OVERRIDE;

  // Disable default constructors
  MasstreeStorage() CXX11_FUNC_DELETE;
  MasstreeStorage(const MasstreeStorage&) CXX11_FUNC_DELETE;
  MasstreeStorage& operator=(const MasstreeStorage&) CXX11_FUNC_DELETE;

  // Initializable interface
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  // Storage interface
  StorageId           get_id()    const CXX11_OVERRIDE;
  StorageType         get_type()  const CXX11_OVERRIDE { return kMasstreeStorage; }
  const std::string&  get_name()  const CXX11_OVERRIDE;
  const Metadata*     get_metadata()  const CXX11_OVERRIDE;
  const MasstreeMetadata*  get_masstree_metadata()  const;
  bool                exists()    const CXX11_OVERRIDE;
  ErrorStack          create(thread::Thread* context) CXX11_OVERRIDE;
  void       describe(std::ostream* o) const CXX11_OVERRIDE;

  /** Use this only if you know what you are doing. */
  MasstreeStoragePimpl*  get_pimpl() { return pimpl_; }

 private:
  MasstreeStoragePimpl*  pimpl_;
};

/**
 * @brief Factory object for masstree storages.
 * @ingroup MASSTREE
 */
class MasstreeStorageFactory CXX11_FINAL : public virtual StorageFactory {
 public:
  ~MasstreeStorageFactory() {}
  StorageType   get_type() const CXX11_OVERRIDE { return kMasstreeStorage; }
  bool          is_right_metadata(const Metadata *metadata) const;
  ErrorStack    get_instance(Engine* engine, const Metadata *metadata, Storage** storage) const;
  void          add_create_log(const Metadata* metadata, thread::Thread* context) const;
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_HPP_
