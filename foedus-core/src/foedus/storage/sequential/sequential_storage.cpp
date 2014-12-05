/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace sequential {

SequentialStorage::SequentialStorage() : Storage<SequentialStorageControlBlock>() {}
SequentialStorage::SequentialStorage(Engine* engine, SequentialStorageControlBlock* control_block)
  : Storage<SequentialStorageControlBlock>(engine, control_block) {
  ASSERT_ND(get_type() == kSequentialStorage || !exists());
}
SequentialStorage::SequentialStorage(Engine* engine, StorageControlBlock* control_block)
  : Storage<SequentialStorageControlBlock>(engine, control_block) {
    ASSERT_ND(get_type() == kSequentialStorage || !exists());
}
SequentialStorage::SequentialStorage(Engine* engine, StorageId id)
  : Storage<SequentialStorageControlBlock>(engine, id) {}
SequentialStorage::SequentialStorage(Engine* engine, const StorageName& name)
  : Storage<SequentialStorageControlBlock>(engine, name) {}
SequentialStorage::SequentialStorage(const SequentialStorage& other)
  : Storage<SequentialStorageControlBlock>(other.engine_, other.control_block_) {
}
SequentialStorage& SequentialStorage::operator=(const SequentialStorage& other) {
  engine_ = other.engine_;
  control_block_ = other.control_block_;
  return *this;
}

const SequentialMetadata* SequentialStorage::get_sequential_metadata() const  {
  return &control_block_->meta_;
}

ErrorStack SequentialStorage::create(const Metadata &metadata) {
  return SequentialStoragePimpl(this).create(static_cast<const SequentialMetadata&>(metadata));
}
ErrorStack SequentialStorage::load(const StorageControlBlock& snapshot_block) {
  return SequentialStoragePimpl(this).load(snapshot_block);
}
ErrorStack SequentialStorage::drop() {
  return SequentialStoragePimpl(this).drop();
}

ErrorStack SequentialStorage::replace_pointers(const Composer::ReplacePointersArguments& args) {
  return SequentialStoragePimpl(this).replace_pointers(args);
}

std::ostream& operator<<(std::ostream& o, const SequentialStorage& v) {
  uint64_t page_count = 0;
  uint64_t record_count = 0;
  SequentialStoragePimpl pimpl(const_cast<SequentialStorage*>(&v));
  pimpl.for_every_page([&page_count, &record_count](SequentialPage* page){
    ++page_count;
    record_count += page->get_record_count();
    return kErrorCodeOk;
  });
  o << "<SequentialStorage>"
    << "<id>" << v.get_id() << "</id>"
    << "<name>" << v.get_name() << "</name>"
    << "<page_count>" << page_count << "</page_count>"
    << "<record_count>" << record_count << "</record_count>"
    << "</SequentialStorage>";
  return o;
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
