/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_HASH_HASH_METADATA_HPP_
#define FOEDUS_STORAGE_HASH_HASH_METADATA_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/fwd.hpp"

namespace foedus {
namespace storage {
namespace hash {
/**
 * @brief Metadata of an hash storage.
 * @ingroup HASH
 */
struct HashMetadata CXX11_FINAL : public virtual Metadata {
  HashMetadata()
    : Metadata(0, kHashStorage, "") {}
  HashMetadata(StorageId id, const std::string& name)
    : Metadata(id, kHashStorage, name) {
  }
  /** This one is for newly creating a storage. */
  HashMetadata(const std::string& name)
    : Metadata(0, kHashStorage, name) {
  }
  EXTERNALIZABLE(HashMetadata);

  Metadata* clone() const CXX11_OVERRIDE;
};
}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_METADATA_HPP_
