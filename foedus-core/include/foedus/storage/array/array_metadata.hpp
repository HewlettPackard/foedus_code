/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_METADATA_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_METADATA_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/fwd.hpp"

namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Metadata of an array storage.
 * @ingroup ARRAY
 */
struct ArrayMetadata CXX11_FINAL : public virtual Metadata {
  ArrayMetadata()
    : Metadata(0, kArrayStorage, ""), payload_size_(0), array_size_(0), root_page_id_(0) {}
  ArrayMetadata(StorageId id, const std::string& name, uint16_t payload_size,
          ArrayOffset array_size, SnapshotPagePointer root_page_id)
    : Metadata(id, kArrayStorage, name),
    payload_size_(payload_size), array_size_(array_size), root_page_id_(root_page_id) {
  }
  /** This one is for newly creating a storage. */
  ArrayMetadata(const std::string& name, uint16_t payload_size, ArrayOffset array_size)
    : Metadata(0, kArrayStorage, name),
    payload_size_(payload_size), array_size_(array_size), root_page_id_(0) {
  }
  EXTERNALIZABLE(ArrayMetadata);

  Metadata* clone() const CXX11_OVERRIDE;

  /** byte size of one record in this array storage without internal overheads */
  uint16_t            payload_size_;
  /** Size of this array */
  ArrayOffset         array_size_;
  SnapshotPagePointer root_page_id_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_METADATA_HPP_
