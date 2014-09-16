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
  SequentialMetadata() : Metadata(0, kSequentialStorage, "") {}
  SequentialMetadata(StorageId id, const StorageName& name)
    : Metadata(id, kSequentialStorage, name) {
  }
  /** This one is for newly creating a storage. */
  explicit SequentialMetadata(const StorageName& name)
    : Metadata(0, kSequentialStorage, name) {
  }
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
