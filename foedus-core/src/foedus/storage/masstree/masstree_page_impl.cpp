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
      DualPagePointer& pointer = layer_record(slot.offset_)->pointer_;
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
  ASSERT_ND(page_version_.get_key_count() == 0);
  const MasstreeBorderPage::Slot& parent_slot = copy_from->get_slot(copy_index);
  ASSERT_ND(parent_slot.remaining_key_length_ > sizeof(KeySlice));  // otherwise why came here
  uint16_t remaining = parent_slot.remaining_key_length_ - sizeof(KeySlice);
  const Record* parent_record = copy_from->body_record(parent_slot.offset_);

  // retrieve the first 8 byte (or less) as the new slice.
  KeySlice new_slice = slice_key(reinterpret_cast<const char*>(parent_record->payload_), remaining);
  uint16_t payload_length = parent_slot.payload_length_;
  uint16_t suffix_length = calculate_suffix_length(remaining);
  uint16_t record_length = calculate_record_size(remaining, payload_length);

  MasstreeBorderPage::Slot& new_slot = get_slot(0);
  new_slot.slice_ = new_slice;
  new_slot.flags_ = 0;
  new_slot.remaining_key_length_ = remaining;
  new_slot.payload_length_ = payload_length;
  new_slot.offset_ = kDataSize - record_length;

  Record* new_record = body_record(new_slot.offset_);
  // use the same xct ID. This means we also inherit deleted flag.
  new_record->owner_id_ = parent_record->owner_id_;
  // but we don't want to inherit locks
  if (new_record->owner_id_.is_keylocked()) {
    new_record->owner_id_.release_keylock();
  }
  if (new_record->owner_id_.is_rangelocked()) {
    new_record->owner_id_.release_rangelock();
  }
  if (suffix_length > 0) {
    std::memcpy(new_record->payload_, parent_record->payload_ + sizeof(KeySlice), suffix_length);
  }
  std::memcpy(
    new_record->payload_ + suffix_length,
    parent_record->payload_ + parent_slot.get_suffix_length(),
    payload_length);

  page_version_.increment_key_count();
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
