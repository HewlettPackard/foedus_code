/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_page_impl.hpp"

#include <cstring>

#include "foedus/epoch.hpp"

namespace foedus {
namespace storage {
namespace array {
void ArrayPage::initialize_snapshot_page(
  StorageId storage_id,
  SnapshotPagePointer page_id,
  uint16_t payload_size,
  uint8_t level,
  const ArrayRange& array_range) {
  std::memset(this, 0, kPageSize);
  header_.init_snapshot(page_id, storage_id, kArrayPageType);
  payload_size_ = payload_size;
  level_ = level;
  array_range_ = array_range;
}

void ArrayPage::initialize_volatile_page(
  Epoch initial_epoch,
  StorageId storage_id,
  VolatilePagePointer page_id,
  uint16_t payload_size,
  uint8_t level,
  const ArrayRange& array_range) {
  std::memset(this, 0, kPageSize);
  header_.init_volatile(page_id, storage_id, kArrayPageType);
  payload_size_ = payload_size;
  level_ = level;
  array_range_ = array_range;
  if (is_leaf()) {
    uint16_t records = get_leaf_record_count();
    for (uint16_t i = 0; i < records; ++i) {
      get_leaf_record(i, payload_size)->owner_id_.xct_id_.set_epoch(initial_epoch);
    }
  }
}

void ArrayVolatileInitializer::initialize_more(Page* page) const {
  ArrayRange range = route_.calculate_page_range(
    page_level_,
    total_levels_,
    payload_size_,
    array_size_);
  VolatilePagePointer volatile_pointer;
  volatile_pointer.word = page->get_header().page_id_;
  ArrayPage* casted = reinterpret_cast<ArrayPage*>(page);
  casted->initialize_volatile_page(
    initial_epoch_,
    storage_id_,
    volatile_pointer,
    payload_size_,
    page_level_,
    range);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
