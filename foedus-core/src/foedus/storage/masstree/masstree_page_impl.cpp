/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_page_impl.hpp"

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

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
