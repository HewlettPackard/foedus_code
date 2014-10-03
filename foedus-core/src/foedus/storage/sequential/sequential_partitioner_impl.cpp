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
  PartitionId local_partition,
  const snapshot::LogBuffer&      /*log_buffer*/,
  const snapshot::BufferPosition* /*log_positions*/,
  uint32_t                        logs_count,
  PartitionId*                    results) const {
  // all local
  for (uint32_t i = 0; i < logs_count; ++i) {
    results[i] = local_partition;
  }
}

void SequentialPartitioner::sort_batch(
    const snapshot::LogBuffer&        /*log_buffer*/,
    const snapshot::BufferPosition*   log_positions,
    uint32_t                          log_positions_count,
    const memory::AlignedMemorySlice& /*sort_buffer*/,
    Epoch                             /*base_epoch*/,
    snapshot::BufferPosition*         output_buffer,
    uint32_t*                         written_count) const {
  // no sorting needed.
  std::memcpy(output_buffer, log_positions, sizeof(snapshot::BufferPosition) * log_positions_count);
  *written_count = log_positions_count;
}


std::ostream& operator<<(std::ostream& o, const SequentialPartitioner& /*v*/) {
  o << "<SequentialPartitioner>"
    << "</SequentialPartitioner>";
  return o;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
