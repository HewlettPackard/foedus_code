/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_partitioner.hpp"

#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace array {

ArrayPartitioner::ArrayPartitioner(Engine* engine, StorageId id) {
  ArrayStorage* storage = dynamic_cast<ArrayStorage*>(
    engine->get_storage_manager().get_storage(id));
  ASSERT_ND(storage);

  array_id_ = storage->get_id();
  array_size_ = storage->get_array_size();
  bucket_size_ = array_size_ / kInteriorFanout;
  bucket_size_div_ = assorted::ConstDiv(bucket_size_);

  ArrayStoragePimpl* array = storage->get_pimpl();
  if (array->levels_ == 1) {
    ASSERT_ND(array->root_page_->is_leaf());
    array_single_page_ = true;
  } else {
    ASSERT_ND(!array->root_page_->is_leaf());
    array_single_page_ = false;

    // how many direct children does this root page have?
    std::vector<uint64_t> pages = ArrayStoragePimpl::calculate_required_pages(
      array_size_, storage->get_payload_size());
    ASSERT_ND(pages.size() == array->levels_);
    uint16_t direct_children = pages.back();

    // do we have enough direct children? if not, some partition will not receive buckets.
    // Although it's not a critical error, let's log it as an error.
    snapshot::PartitionId total_partitions =
      engine->get_options().thread_.group_count_ *
      engine->get_options().snapshot_.partitions_per_node_;

    if (direct_children < total_partitions) {
      LOG(ERROR) << "Warning-like error: This array doesn't have enough direct children in root"
        " page to assign partitions. #partitions=" << total_partitions << ", #direct children="
        << direct_children << ". array=" << *storage;
    }

    // two paths. first path simply sees volatile/snapshot pointer and determines owner.
    // second path addresses excessive assignments, off loading them to needy ones.
    std::vector<uint16_t> counts(total_partitions, 0);
    const uint16_t excessive_count = (direct_children * 12 / (total_partitions * 10)) + 1;
    std::vector<uint16_t> excessive_children;
    for (uint16_t child = 0; child < direct_children; ++child) {
      thread::ThreadGroupId group = 0;

      const DualPagePointer &pointer = array->root_page_->get_interior_record(child);
      if (pointer.volatile_pointer_.components.offset != 0) {
        group = pointer.volatile_pointer_.components.numa_node;
      } else {
        // if no volatile page, see snapshot page owner.
        group = extract_numa_node_from_snapshot_pointer(pointer.snapshot_pointer_);
        // this ignores the case where neither snapshot/volatile page is there.
        // however, as we create all pages at ArrayStorage::create(), this so far never happens.
      }
      ASSERT_ND(group < engine->get_options().thread_.group_count_);
      // snapshot::PartitionId
    }
  }
}

void ArrayPartitioner::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<ArrayPartitioner>"
      << "<array_id_>" << array_id_ << "</array_id_>"
      << "<array_size_>" << array_size_ << "</array_size_>"
      << "<bucket_size_>" << bucket_size_ << "</bucket_size_>";
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    o << "<range bucket=\"" << i << "\" partition=\"" << bucket_owners_[i] << "\" />";
  }
  o << "</ArrayPartitioner>";
}

ErrorStack ArrayPartitioner::partition_batch(
  const log::RecordLogType** logs,
  uint32_t logs_count,
  snapshot::PartitionId* results) const {
  if (array_single_page_) {
    // then no partitioning possible.
    for (uint32_t i = 0; i < logs_count; ++i) {
      results[i] = 0;
    }
    return kRetOk;
  }

  for (uint32_t i = 0; i < logs_count; ++i) {
    ASSERT_ND(logs[i]->header_.log_type_code_ == log::kLogCodeArrayOverwrite);
    ASSERT_ND(logs[i]->header_.storage_id_ == array_id_);
    const OverwriteLogType *log = reinterpret_cast<const OverwriteLogType*>(logs[i]);
    ASSERT_ND(log->offset_ < array_size_);
    uint64_t bucket = bucket_size_div_.div64(log->offset_);
    ASSERT_ND(bucket < kInteriorFanout);
    results[i] = bucket_owners_[bucket];
  }

  return kRetOk;
}
}  // namespace array
}  // namespace storage
}  // namespace foedus
