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
struct ArrayMetadata CXX11_FINAL : public Metadata {
  enum Constants {
    kDefaultSnapshotDropVolatilePagesThreshold = 3,
  };
  ArrayMetadata()
    : Metadata(0, kArrayStorage, ""),
    payload_size_(0),
    snapshot_drop_volatile_pages_threshold_(kDefaultSnapshotDropVolatilePagesThreshold),
    padding_(0),
    array_size_(0) {}
  ArrayMetadata(
    StorageId id,
    const StorageName& name,
    uint16_t payload_size,
    ArrayOffset array_size)
    : Metadata(id, kArrayStorage, name),
    payload_size_(payload_size),
    snapshot_drop_volatile_pages_threshold_(kDefaultSnapshotDropVolatilePagesThreshold),
    padding_(0),
    array_size_(array_size) {
  }
  /** This one is for newly creating a storage. */
  ArrayMetadata(const StorageName& name, uint16_t payload_size, ArrayOffset array_size)
    : Metadata(0, kArrayStorage, name),
    payload_size_(payload_size),
    snapshot_drop_volatile_pages_threshold_(kDefaultSnapshotDropVolatilePagesThreshold),
    padding_(0),
    array_size_(array_size) {
  }

  std::string describe() const;
  friend std::ostream& operator<<(std::ostream& o, const ArrayMetadata& v);

  /** byte size of one record in this array storage without internal overheads */
  uint16_t            payload_size_;
  /**
   * Number of levels of volatile pages to keep after each snapshotting.
   * 0 means this storage keeps no volatile pages after snapshotting.
   * 1 means it keeps only the root page, 2 means another level, ...
   * The default is 3. Keeping 256^2=64k pages in higher level should hit a good balance.
   * If it doesn't, the user (you) chooses the right value per storage.
   */
  uint16_t            snapshot_drop_volatile_pages_threshold_;
  uint32_t            padding_;  // to make valgrind happy
  /** Size of this array */
  ArrayOffset         array_size_;
};

struct ArrayMetadataSerializer CXX11_FINAL : public virtual MetadataSerializer {
  ArrayMetadataSerializer() : MetadataSerializer() {}
  explicit ArrayMetadataSerializer(ArrayMetadata* data)
    : MetadataSerializer(data), data_casted_(data) {}
  EXTERNALIZABLE(ArrayMetadataSerializer);
  ArrayMetadata* data_casted_;
};

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_METADATA_HPP_
