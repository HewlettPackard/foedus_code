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
    : Metadata(0, kArrayStorage, ""), payload_size_(0), array_size_(0) {}
  ArrayMetadata(
    StorageId id,
    const StorageName& name,
    uint16_t payload_size,
    ArrayOffset array_size)
    : Metadata(id, kArrayStorage, name),
    payload_size_(payload_size), array_size_(array_size) {
  }
  /** This one is for newly creating a storage. */
  ArrayMetadata(const StorageName& name, uint16_t payload_size, ArrayOffset array_size)
    : Metadata(0, kArrayStorage, name),
    payload_size_(payload_size), array_size_(array_size) {
  }
  explicit ArrayMetadata(const ArrayMetadata& other)
    : Metadata(other), payload_size_(other.payload_size_), array_size_(other.array_size_) {
  }
  ArrayMetadata& operator=(const ArrayMetadata& other) {
    id_ = other.id_;
    type_ = other.type_;
    name_ = other.name_;
    root_snapshot_page_id_ = other.root_snapshot_page_id_;
    payload_size_ = other.payload_size_;
    array_size_ = other.array_size_;
    return *this;
  }
  EXTERNALIZABLE(ArrayMetadata);


  Metadata* clone() const CXX11_OVERRIDE;

  /** byte size of one record in this array storage without internal overheads */
  uint16_t            payload_size_;
  /** Size of this array */
  ArrayOffset         array_size_;
};

struct FixedArrayMetadata CXX11_FINAL : public FixedMetadata {
  FixedArrayMetadata()
    : FixedMetadata(0, kArrayStorage, ""), payload_size_(0), array_size_(0) {}
  FixedArrayMetadata(
    StorageId id,
    const StorageName& name,
    uint16_t payload_size,
    ArrayOffset array_size)
    : FixedMetadata(id, kArrayStorage, name),
    payload_size_(payload_size), array_size_(array_size) {
  }
  /** This one is for newly creating a storage. */
  FixedArrayMetadata(const StorageName& name, uint16_t payload_size, ArrayOffset array_size)
    : FixedMetadata(0, kArrayStorage, name),
    payload_size_(payload_size), array_size_(array_size) {
  }

  /** byte size of one record in this array storage without internal overheads */
  uint16_t            payload_size_;
  /** Size of this array */
  ArrayOffset         array_size_;
};

struct ArrayMetadataSerializer CXX11_FINAL : public virtual MetadataSerializer {
  ArrayMetadataSerializer() : MetadataSerializer() {}
  explicit ArrayMetadataSerializer(FixedArrayMetadata* data)
    : MetadataSerializer(data), data_casted_(data) {}
  EXTERNALIZABLE(ArrayMetadataSerializer);
  FixedArrayMetadata* data_casted_;
};

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_METADATA_HPP_
