/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_storage.hpp"

#include <glog/logging.h>

#include <iostream>
#include <string>

#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace sequential {
bool        SequentialStorage::exists()           const  { return control_block_->exists(); }
StorageId   SequentialStorage::get_id()           const  { return control_block_->meta_.id_; }
StorageType SequentialStorage::get_type()         const  { return control_block_->meta_.type_; }
const StorageName& SequentialStorage::get_name()  const  { return control_block_->meta_.name_; }
const Metadata* SequentialStorage::get_metadata() const  { return &control_block_->meta_; }
const SequentialMetadata* SequentialStorage::get_sequential_metadata() const  {
  return &control_block_->meta_;
}

ErrorStack SequentialStorage::create(const Metadata &metadata) {
  return SequentialStoragePimpl(this).create(static_cast<const SequentialMetadata&>(metadata));
}

ErrorStack SequentialStorage::drop() {
  return SequentialStoragePimpl(this).drop();
}

void SequentialStorage::describe(std::ostream* o_ptr) const {
  std::ostream& o = *o_ptr;
  uint64_t page_count = 0;
  uint64_t record_count = 0;
  SequentialStoragePimpl pimpl(const_cast<SequentialStorage*>(this));
  pimpl.for_every_page([&page_count, &record_count](SequentialPage* page){
    ++page_count;
    record_count += page->get_record_count();
    return kErrorCodeOk;
  });
  o << "<SequentialStorage>"
    << "<id>" << get_id() << "</id>"
    << "<name>" << get_name() << "</name>"
    << "<page_count>" << page_count << "</page_count>"
    << "<record_count>" << record_count << "</record_count>"
    << "</SequentialStorage>";
}

// most other methods are defined in pimpl.cpp to allow inlining

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
