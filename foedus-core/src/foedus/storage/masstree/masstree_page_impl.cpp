/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_page_impl.hpp"

#include "foedus/memory/page_pool.hpp"

namespace foedus {
namespace storage {
namespace masstree {

void MasstreePage::initialize_volatile_common(
  StorageId storage_id,
  VolatilePagePointer page_id,
  PageType page_type,
  uint8_t layer,
  MasstreePage* parent) {
  header_.init_volatile(
    page_id,
    storage_id,
    page_type,
    parent == nullptr,
    reinterpret_cast<Page*>(parent));
  uint64_t ver = (layer << kPageVersionLayerShifts);
  if (parent == nullptr || parent->get_layer() == layer - 1) {
    in_layer_parent_ = nullptr;
    ver |= kPageVersionIsRootBit;
  } else {
    ASSERT_ND(parent->header_.get_page_type() == kMasstreeIntermediatePageType);
    in_layer_parent_ = reinterpret_cast<MasstreeIntermediatePage*>(parent);
  }
  if (page_type == kMasstreeBorderPageType) {
    ver |= kPageVersionIsBorderBit;
  }
  page_version_.set_data(ver);
}

void MasstreeBorderPage::initialize_volatile_page(
  StorageId storage_id,
  VolatilePagePointer page_id,
  uint8_t layer,
  MasstreePage* parent) {
  // parent can be either border (in another layer) or intermediate (in same layer).
  ASSERT_ND(
    (parent == nullptr && layer == 0)
    ||
    (parent->get_layer() == layer
      && parent->header().get_page_type() == kMasstreeIntermediatePageType)
    ||
    (parent->get_layer() == layer - 1
      && parent->header().get_page_type() == kMasstreeBorderPageType));
  initialize_volatile_common(storage_id, page_id, kMasstreeBorderPageType, layer, parent);
}

void MasstreePage::release_pages_recursive_common(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  if (header_.get_page_type() == kMasstreeBorderPageType) {
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(this);
    casted->release_pages_recursive(page_resolver, batch);
  } else {
    ASSERT_ND(header_.get_page_type() == kMasstreeIntermediatePageType);
    MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(this);
    casted->release_pages_recursive(page_resolver, batch);
  }
}

void MasstreeIntermediatePage::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  uint16_t key_count = get_version().get_key_count();
  ASSERT_ND(key_count <= kMaxIntermediateSeparators);
  for (uint8_t i = 0; i < key_count + 1; ++i) {
    MiniPage& minipage = get_minipage(i);
    uint16_t mini_count = minipage.mini_version_.get_key_count();
    ASSERT_ND(mini_count <= kMaxIntermediateMiniSeparators);
    for (uint8_t j = 0; j < mini_count + 1; ++j) {
      VolatilePagePointer& pointer = minipage.pointers_[j].volatile_pointer_;
      if (pointer.components.offset != 0) {
        MasstreePage* child = reinterpret_cast<MasstreePage*>(
          page_resolver.resolve_offset(pointer));
        child->release_pages_recursive_common(page_resolver, batch);
        pointer.components.offset = 0;
      }
    }
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  batch->release(volatile_id);
}

void MasstreeBorderPage::release_pages_recursive(
  const memory::GlobalVolatilePageResolver& page_resolver,
  memory::PageReleaseBatch* batch) {
  uint16_t key_count = get_version().get_key_count();
  ASSERT_ND(key_count <= kMaxKeys);
  for (uint8_t i = 0; i < key_count; ++i) {
    const Slot& slot = get_slot(i);
    if (slot.does_point_to_layer()) {
      DualPagePointer& pointer = layer_record(slot.offset_);
      if (pointer.volatile_pointer_.components.offset != 0) {
        MasstreePage* child = reinterpret_cast<MasstreePage*>(
          page_resolver.resolve_offset(pointer.volatile_pointer_));
        child->release_pages_recursive_common(page_resolver, batch);
        pointer.volatile_pointer_.components.offset = 0;
      }
    }
  }

  VolatilePagePointer volatile_id;
  volatile_id.word = header().page_id_;
  batch->release(volatile_id);
}


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
