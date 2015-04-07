/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
  enum Constants {
    kDefaultDropVolatilePagesBtreeLevels = 3,
  };
  MasstreeMetadata() :
    Metadata(0, kMasstreeStorage, ""),
    border_early_split_threshold_(0),
    snapshot_drop_volatile_pages_layer_threshold_(0),
    snapshot_drop_volatile_pages_btree_levels_(kDefaultDropVolatilePagesBtreeLevels),
    pad1_(0) {}
  MasstreeMetadata(
    StorageId id,
    const StorageName& name,
    uint16_t border_early_split_threshold = 0,
    uint16_t snapshot_drop_volatile_pages_layer_threshold = 0,
    uint16_t snapshot_drop_volatile_pages_btree_levels = kDefaultDropVolatilePagesBtreeLevels)
    : Metadata(id, kMasstreeStorage, name),
      border_early_split_threshold_(border_early_split_threshold),
      snapshot_drop_volatile_pages_layer_threshold_(snapshot_drop_volatile_pages_layer_threshold),
      snapshot_drop_volatile_pages_btree_levels_(snapshot_drop_volatile_pages_btree_levels),
      pad1_(0) {
  }
  /** This one is for newly creating a storage. */
  MasstreeMetadata(
    const StorageName& name,
    uint16_t border_early_split_threshold = 0,
    uint16_t snapshot_drop_volatile_pages_layer_threshold = 0,
    uint16_t snapshot_drop_volatile_pages_btree_levels = kDefaultDropVolatilePagesBtreeLevels)
    : Metadata(0, kMasstreeStorage, name),
      border_early_split_threshold_(border_early_split_threshold),
      snapshot_drop_volatile_pages_layer_threshold_(snapshot_drop_volatile_pages_layer_threshold),
      snapshot_drop_volatile_pages_btree_levels_(snapshot_drop_volatile_pages_btree_levels),
      pad1_(0) {
  }

  std::string describe() const;
  friend std::ostream& operator<<(std::ostream& o, const MasstreeMetadata& v);

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

  /**
   * Number of B-trie layers of volatile pages to keep after each snapshotting.
   * 0 means this storage drops volatile pages even if it's in the first layer.
   * 1 means it keeps all pages in first layer.
   * The default is 0.
   */
  uint16_t snapshot_drop_volatile_pages_layer_threshold_;
  /**
   * Volatile pages of this B-tree level or higher are always kept after each snapshotting.
   * 0 means we don't drop any volatile pages.
   * 1 means we drop only border pages.
   * Note that this and snapshot_drop_volatile_pages_layer_threshold_ are AND conditions,
   * meaning we drop volatile pages that meet both conditions.
   * Further, we anyway don't drop volatile pages that have modifications after the snapshot epoch.
   * The default is kDefaultDropVolatilePagesBtreeLevels.
   */
  uint16_t snapshot_drop_volatile_pages_btree_levels_;

  // just for valgrind when this metadata is written to file. ggr
  uint16_t pad1_;
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
