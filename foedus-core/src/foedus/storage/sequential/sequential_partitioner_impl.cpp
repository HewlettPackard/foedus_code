/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"

#include <cstring>
#include <ostream>

namespace foedus {
namespace storage {
namespace sequential {

SequentialPartitioner::SequentialPartitioner(Partitioner* parent)
  : engine_(parent->get_engine()),
    id_(parent->get_storage_id()),
    metadata_(PartitionerMetadata::get_metadata(engine_, id_)) {
}

ErrorStack SequentialPartitioner::design_partition() {
  // no data required for SequentialPartitioner
  metadata_->data_offset_ = 0;
  metadata_->data_size_ = 0;
  metadata_->valid_ = true;
  return kRetOk;
}

void SequentialPartitioner::partition_batch(
  const Partitioner::PartitionBatchArguments& args) const {
  // all local
  for (uint32_t i = 0; i < args.logs_count_; ++i) {
    args.results_[i] = args.local_partition_;
  }
}

void SequentialPartitioner::sort_batch(const Partitioner::SortBatchArguments& args) const {
  // no sorting needed.
  std::memcpy(
    args.output_buffer_,
    args.log_positions_,
    sizeof(snapshot::BufferPosition) * args.logs_count_);
  *args.written_count_ = args.logs_count_;
}


std::ostream& operator<<(std::ostream& o, const SequentialPartitioner& /*v*/) {
  o << "<SequentialPartitioner>"
    << "</SequentialPartitioner>";
  return o;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
