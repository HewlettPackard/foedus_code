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
 * @details
 * @par About root_snapshot_page_id
 * In sequential storage, root_snapshot_page_id points to the \e head of stable root pages.
 * A sequential storage has zero or more stable root pages (SequentialRootPage) that form a
 * singly-linked list. Each of them contains a number of page pointers to head pages.
 * In reality, most sequential storage should have only one root page which has only a few
 * head pages.
 *
 * When this pointer is zero, there is no stable head page.
 *
 * This page pointer is not dual page pointer because we never have volatile (modify-able)
 * root pages. All the volatile part are stored as the in-memory append-only list, which is
 * totally orthogonal to snapshot pages.
 */
struct SequentialMetadata CXX11_FINAL : public Metadata {
  SequentialMetadata()
    : Metadata(0, kSequentialStorage, ""), truncate_epoch_(Epoch::kEpochInvalid), padding_(0) {}
  SequentialMetadata(StorageId id, const StorageName& name)
    : Metadata(id, kSequentialStorage, name), truncate_epoch_(Epoch::kEpochInvalid), padding_(0) {
  }
  /** This one is for newly creating a storage. */
  explicit SequentialMetadata(const StorageName& name)
    : Metadata(0, kSequentialStorage, name), truncate_epoch_(Epoch::kEpochInvalid), padding_(0) {
  }

  std::string describe() const;
  friend std::ostream& operator<<(std::ostream& o, const SequentialMetadata& v);

  uint32_t unused_dummy_func_padding() const { return padding_; }

  /**
   * The min epoch value (\e truncate-epoch) for all valid records in this storage.
   * When a physical record or a page has an epoch value less than a truncate-epoch,
   * they are logically non-existent. Truncate-epoch is always valid, starting from the system's
   * lowest epoch.
   */
  Epoch::EpochInteger truncate_epoch_;

  uint32_t  padding_;
};

struct SequentialMetadataSerializer CXX11_FINAL : public virtual MetadataSerializer {
  SequentialMetadataSerializer() : MetadataSerializer() {}
  explicit SequentialMetadataSerializer(SequentialMetadata* data)
    : MetadataSerializer(data), data_casted_(data) {}
  EXTERNALIZABLE(SequentialMetadataSerializer);
  SequentialMetadata* data_casted_;
};

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_METADATA_HPP_
