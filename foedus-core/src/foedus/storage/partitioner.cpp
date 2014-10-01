/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/partitioner.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

namespace foedus {
namespace storage {

PartitionerMetadata* PartitionerMetadata::get_metadata(Engine* engine, StorageId id) {
  ASSERT_ND(id > 0);
  soc::GlobalMemoryAnchors* anchors
    = engine->get_soc_manager()->get_shared_memory_repo()->get_global_memory_anchors();
  return anchors->partitioner_metadata_ + id;
}

PartitionerMetadata* PartitionerMetadata::get_index0_metadata(Engine* engine) {
  soc::GlobalMemoryAnchors* anchors
    = engine->get_soc_manager()->get_shared_memory_repo()->get_global_memory_anchors();
  return anchors->partitioner_metadata_;
}

Partitioner::Partitioner(Engine* engine, StorageId id)
  : Attachable< PartitionerMetadata >(engine, PartitionerMetadata::get_metadata(engine, id)) {
  id_ = id;
  type_ = engine->get_storage_manager()->get_storage(id)->meta_.type_;
  ASSERT_ND(type_ != kInvalidStorage);
}

const PartitionerMetadata& Partitioner::get_metadata() const { return *control_block_; }

bool Partitioner::is_partitionable() {
  switch (type_) {
  case kArrayStorage:
    return array::ArrayPartitioner(this).is_partitionable();
  case kHashStorage:
    return 0;
  case kMasstreeStorage:
    return 0;
  case kSequentialStorage:
    return sequential::SequentialPartitioner(this).is_partitionable();
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
    return false;
  }
}

void Partitioner::design_partition() {
  switch (type_) {
  case kArrayStorage:
    array::ArrayPartitioner(this).design_partition();
    break;
  case kHashStorage:
    break;
  case kMasstreeStorage:
    break;
  case kSequentialStorage:
    sequential::SequentialPartitioner(this).design_partition();
    break;
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
  }
}

void Partitioner::partition_batch(
  PartitionId                     local_partition,
  const snapshot::LogBuffer&      log_buffer,
  const snapshot::BufferPosition* log_positions,
  uint32_t                        logs_count,
  PartitionId*                    results) {
  switch (type_) {
  case kArrayStorage:
    array::ArrayPartitioner(this).partition_batch(
      local_partition,
      log_buffer,
      log_positions,
      logs_count,
      results);
    break;
  case kHashStorage:
    break;
  case kMasstreeStorage:
    break;
  case kSequentialStorage:
    sequential::SequentialPartitioner(this).partition_batch(
      local_partition,
      log_buffer,
      log_positions,
      logs_count,
      results);
    break;
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
  }
}

void Partitioner::sort_batch(
  const snapshot::LogBuffer&        log_buffer,
  const snapshot::BufferPosition*   log_positions,
  uint32_t                          logs_count,
  const memory::AlignedMemorySlice& sort_buffer,
  Epoch                             base_epoch,
  snapshot::BufferPosition*         output_buffer,
  uint32_t*                         written_count) {
  switch (type_) {
  case kArrayStorage:
    array::ArrayPartitioner(this).sort_batch(
      log_buffer,
      log_positions,
      logs_count,
      sort_buffer,
      base_epoch,
      output_buffer,
      written_count);
    break;
  case kHashStorage:
    break;
  case kMasstreeStorage:
    break;
  case kSequentialStorage:
    sequential::SequentialPartitioner(this).sort_batch(
      log_buffer,
      log_positions,
      logs_count,
      sort_buffer,
      base_epoch,
      output_buffer,
      written_count);
    break;
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
  }
}

uint64_t Partitioner::get_required_sort_buffer_size(uint32_t log_count) {
  switch (type_) {
  case kArrayStorage:
    return array::ArrayPartitioner(this).get_required_sort_buffer_size(log_count);
  case kHashStorage:
    return 0;
  case kMasstreeStorage:
    return 0;
  case kSequentialStorage:
    return sequential::SequentialPartitioner(this).get_required_sort_buffer_size(log_count);
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
    return 0;
  }
}


std::ostream& operator<<(std::ostream& o, const Partitioner& v) {
  o << "<Partitioner>"
    << "<id>" << v.id_ << "</id>"
    << "<type>" << to_storage_type_name(v.type_) << "</type>"
    << *v.control_block_;
  o << "</Partitioner>";
  return o;
}
std::ostream& operator<<(std::ostream& o, const PartitionerMetadata& v) {
  o << "<PartitionerMetadata>"
    << "<valid>" << v.valid_ << "</valid>"
    << "<data_offset_>" << assorted::Hex(v.data_offset_) << "</data_offset_>"
    << "<data_size_>" << assorted::Hex(v.data_size_) << "</data_size_>"
    << "</PartitionerMetadata>";
  return o;
}

}  // namespace storage
}  // namespace foedus
