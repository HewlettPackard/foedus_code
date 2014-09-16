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
struct MasstreeMetadata CXX11_FINAL : public Metadata {
  MasstreeMetadata() :
    Metadata(0, kMasstreeStorage, ""), border_early_split_threshold_(0) {}
  MasstreeMetadata(
    StorageId id,
    const StorageName& name,
    uint16_t border_early_split_threshold = 0)
    : Metadata(id, kMasstreeStorage, name),
      border_early_split_threshold_(border_early_split_threshold) {
  }
  /** This one is for newly creating a storage. */
  MasstreeMetadata(const StorageName& name, uint16_t border_early_split_threshold = 0)
    : Metadata(0, kMasstreeStorage, name),
      border_early_split_threshold_(border_early_split_threshold) {
  }

  /**
   * @brief Kind of fill factor for border pages, bit different from usual B-tree.
   * @details
   * Border pages split without being full when a border page seems to receive sequential inserts
   * and the physical key count will exactly hit this value.
   * Once it passes this value, it goes on until it really becomes full
   * (otherwise there is no point.. the border pages keep splitting without necessity).
   * When the page is not receiving sequential inserts, there are also no points to split early.
   * The default is 0, which means we never consider early split.
   */
  uint16_t border_early_split_threshold_;
};

struct MasstreeMetadataSerializer CXX11_FINAL : public virtual MetadataSerializer {
  MasstreeMetadataSerializer() : MetadataSerializer() {}
  explicit MasstreeMetadataSerializer(MasstreeMetadata* data)
    : MetadataSerializer(data), data_casted_(data) {}
  EXTERNALIZABLE(MasstreeMetadataSerializer);
  MasstreeMetadata* data_casted_;
};


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_METADATA_HPP_
