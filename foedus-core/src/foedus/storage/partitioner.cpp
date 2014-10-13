/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/partitioner.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"

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

void* PartitionerMetadata::locate_data(Engine* engine) {
  ASSERT_ND(data_size_ > 0);
  ASSERT_ND(data_offset_ + data_size_
    <= engine->get_options().storage_.partitioner_data_memory_mb_ * (1ULL << 20));
  soc::GlobalMemoryAnchors* anchors
    = engine->get_soc_manager()->get_shared_memory_repo()->get_global_memory_anchors();
  char* buffer = reinterpret_cast<char*>(anchors->partitioner_data_);
  return buffer + data_offset_;
}

ErrorCode PartitionerMetadata::allocate_data(
  Engine* engine,
  soc::SharedMutexScope* locked,
  uint32_t data_size) {
  ASSERT_ND(!valid_);
  ASSERT_ND(data_size > 0);
  ASSERT_ND(data_size_ == 0);
  ASSERT_ND(data_offset_ == 0);
  ASSERT_ND(locked->get_mutex() == &mutex_);
  ASSERT_ND(locked->is_locked_by_me());
  PartitionerMetadata* index0 = get_index0_metadata(engine);
  ASSERT_ND(index0->data_offset_ <= index0->data_size_);
  soc::SharedMutexScope index0_scope(&index0->mutex_);
  if (index0->data_offset_ + data_size > index0->data_size_) {
    return kErrorCodeStrPartitionerDataMemoryTooSmall;
  }
  data_offset_ = index0->data_offset_;
  data_size_ = data_size;
  index0->data_offset_ += data_size;
  return kErrorCodeOk;
}


Partitioner::Partitioner(Engine* engine, StorageId id)
  : Attachable< PartitionerMetadata >(engine, PartitionerMetadata::get_metadata(engine, id)) {
  id_ = id;
  type_ = engine->get_storage_manager()->get_storage(id)->meta_.type_;
  ASSERT_ND(type_ != kInvalidStorage);
}

const PartitionerMetadata& Partitioner::get_metadata() const { return *control_block_; }
bool  Partitioner::is_valid() const { return control_block_->valid_; }

bool Partitioner::is_partitionable() {
  switch (type_) {
  case kArrayStorage: return array::ArrayPartitioner(this).is_partitionable();
  case kHashStorage: return 0;
  case kMasstreeStorage: return 0;
  case kSequentialStorage: return sequential::SequentialPartitioner(this).is_partitionable();
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
    return false;
  }
}

ErrorStack Partitioner::design_partition(const DesignPartitionArguments& args) {
  switch (type_) {
  case kArrayStorage: return array::ArrayPartitioner(this).design_partition(args);
  case kSequentialStorage: return sequential::SequentialPartitioner(this).design_partition(args);
  case kMasstreeStorage: return masstree::MasstreePartitioner(this).design_partition(args);
  case kHashStorage:
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
    return kRetOk;
  }
}

void Partitioner::partition_batch(const Partitioner::PartitionBatchArguments& args) {
  switch (type_) {
  case kArrayStorage: return array::ArrayPartitioner(this).partition_batch(args);
  case kHashStorage:
    break;
  case kMasstreeStorage: return masstree::MasstreePartitioner(this).partition_batch(args);
  case kSequentialStorage: return sequential::SequentialPartitioner(this).partition_batch(args);
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
  }
}

void Partitioner::sort_batch(const Partitioner::SortBatchArguments& args) {
  switch (type_) {
  case kArrayStorage: return array::ArrayPartitioner(this).sort_batch(args);
  case kHashStorage:
    break;
  case kMasstreeStorage: return masstree::MasstreePartitioner(this).sort_batch(args);
  case kSequentialStorage: return sequential::SequentialPartitioner(this).sort_batch(args);
  default:
    LOG(FATAL) << "Unsupported storage type:" << type_;
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
