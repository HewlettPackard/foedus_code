/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_METADATA_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_METADATA_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Metadata of a masstree storage.
 * @ingroup MASSTREE
 * @details
 */
struct MasstreeMetadata CXX11_FINAL : public virtual Metadata {
  MasstreeMetadata() : Metadata(0, kMasstreeStorage, "") {}
  MasstreeMetadata(StorageId id, const std::string& name)
    : Metadata(id, kMasstreeStorage, name) {
  }
  /** This one is for newly creating a storage. */
  explicit MasstreeMetadata(const std::string& name) : Metadata(0, kArrayStorage, name) {
  }
  EXTERNALIZABLE(MasstreeMetadata);

  Metadata* clone() const CXX11_OVERRIDE;
};
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_METADATA_HPP_
