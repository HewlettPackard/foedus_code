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
    parent == nullptr);
  uint64_t ver = (layer << kPageVersionLayerShifts);
  if (page_type == kMasstreeBorderPageType) {
    ver |= kPageVersionIsBorderBit;
  }
  header_.page_version_.set_data(ver);
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
  if (foster_child_) {
    reinterpret_cast<MasstreeIntermediatePage*>(foster_child_)->release_pages_recursive(
      page_resolver,
      batch);
    foster_child_ = nullptr;
  }
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
  if (foster_child_) {
    reinterpret_cast<MasstreeBorderPage*>(foster_child_)->release_pages_recursive(
      page_resolver,
      batch);
    foster_child_ = nullptr;
  }
  uint16_t key_count = get_version().get_key_count();
  ASSERT_ND(key_count <= kMaxKeys);
  for (uint8_t i = 0; i < key_count; ++i) {
    if (does_point_to_layer(i)) {
      DualPagePointer& pointer = *get_next_layer(i);
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


void MasstreeBorderPage::copy_initial_record(
  const MasstreeBorderPage* copy_from,
  uint8_t copy_index) {
  ASSERT_ND(header_.page_version_.get_key_count() == 0);
  uint8_t parent_key_length = copy_from->remaining_key_length_[copy_index];
  ASSERT_ND(parent_key_length != kKeyLengthNextLayer);
  ASSERT_ND(parent_key_length > sizeof(KeySlice));
  uint8_t remaining = parent_key_length - sizeof(KeySlice);

  // retrieve the first 8 byte (or less) as the new slice.
  const char* parent_record = copy_from->get_record(copy_index);
  KeySlice new_slice = slice_key(parent_record, remaining);
  uint16_t payload_length = copy_from->payload_length_[copy_index];
  uint8_t suffix_length = calculate_suffix_length(remaining);

  slices_[0] = new_slice;
  remaining_key_length_[0] = remaining;
  payload_length_[0] = payload_length;
  offsets_[0] = (kDataSize - calculate_record_size(remaining, payload_length)) >> 4;

  // use the same xct ID. This means we also inherit deleted flag.
  owner_ids_[0] = copy_from->owner_ids_[copy_index];
  // but we don't want to inherit locks
  if (owner_ids_[0].is_keylocked()) {
    owner_ids_[0].release_keylock();
  }
  if (owner_ids_[0].is_rangelocked()) {
    owner_ids_[0].release_rangelock();
  }
  if (suffix_length > 0) {
    std::memcpy(get_record(0), parent_record + sizeof(KeySlice), suffix_length);
  }
  std::memcpy(
    get_record(0) + suffix_length,
    parent_record + remaining,
    payload_length);

  header_.page_version_.increment_key_count();
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
