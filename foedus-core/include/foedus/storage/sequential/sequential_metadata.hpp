/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_METADATA_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_METADATA_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/**
 * @brief Metadata of a sequential storage.
 * @ingroup SEQUENTIAL
 */
struct SequentialMetadata CXX11_FINAL : public virtual Metadata {
  SequentialMetadata()
    : Metadata(0, kSequentialStorage, ""), root_page_id_(0) {}
  SequentialMetadata(StorageId id, const std::string& name, SnapshotPagePointer root_page_id)
    : Metadata(id, kSequentialStorage, name), root_page_id_(root_page_id) {
  }
  /** This one is for newly creating a storage. */
  explicit SequentialMetadata(const std::string& name)
    : Metadata(0, kArrayStorage, name), root_page_id_(0) {
  }
  EXTERNALIZABLE(SequentialMetadata);

  Metadata* clone() const CXX11_OVERRIDE;

  SnapshotPagePointer root_page_id_;
};
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_METADATA_HPP_
