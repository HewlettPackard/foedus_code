/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_page_impl.hpp"

#include <cstring>

#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

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

void array_volatile_page_init(const VolatilePageInitArguments& args) {
  ASSERT_ND(args.parent_);
  ASSERT_ND(args.page_);
  ASSERT_ND(args.index_in_parent_ < kInteriorFanout);
  const ArrayPage* parent = reinterpret_cast<const ArrayPage*>(args.parent_);
  ArrayPage* page = reinterpret_cast<ArrayPage*>(args.page_);

  Engine* engine = args.context_->get_engine();
  StorageId storage_id = parent->get_storage_id();
  ArrayStorage storage(engine, storage_id);
  ASSERT_ND(storage.exists());
  const ArrayStorageControlBlock* cb = storage.get_control_block();

  ArrayRange parent_range = parent->get_array_range();
  uint8_t parent_level = parent->get_level();
  ASSERT_ND(parent_level > 0);
  ASSERT_ND(parent_range.end_ - parent_range.begin_ == cb->intervals_[parent_level]
    || parent_range.end_ == storage.get_array_size());

  uint8_t child_level = parent_level - 1U;
  uint64_t interval = cb->intervals_[child_level];
  ASSERT_ND(interval > 0);
  ArrayRange child_range;
  child_range.begin_ = parent_range.begin_ + args.index_in_parent_ * interval;
  child_range.end_ = child_range.begin_ + interval;
  if (child_range.end_ > storage.get_array_size()) {
    child_range.end_ = storage.get_array_size();
  }
  ASSERT_ND(child_range.end_ > 0);

  // Because this is a new page, records in it are in the earliest epoch of the system
  Epoch initial_epoch = engine->get_savepoint_manager()->get_initial_current_epoch();
  page->initialize_volatile_page(
    initial_epoch,
    storage_id,
    args.page_id,
    storage.get_payload_size(),
    child_level,
    child_range);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
