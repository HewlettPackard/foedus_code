/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"

#include <cstring>
#include <ostream>

namespace foedus {
namespace storage {
namespace masstree {

void MasstreePartitioner::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<MasstreePartitioner>"
    << "<id_>" << masstree_id_ << "</id_>"
    << "</MasstreePartitioner>";
}

void MasstreePartitioner::partition_batch(
  PartitionId local_partition,
  const snapshot::LogBuffer&      /*log_buffer*/,
  const snapshot::BufferPosition* /*log_positions*/,
  uint32_t                        logs_count,
  PartitionId*                    results) const {
  // TODO(Hideaki) implement
  for (uint32_t i = 0; i < logs_count; ++i) {
    results[i] = local_partition;
  }
}

void MasstreePartitioner::sort_batch(
    const snapshot::LogBuffer&        /*log_buffer*/,
    const snapshot::BufferPosition*   log_positions,
    uint32_t                          log_positions_count,
    const memory::AlignedMemorySlice& /*sort_buffer*/,
    Epoch                             /*base_epoch*/,
    snapshot::BufferPosition*         output_buffer,
    uint32_t*                         written_count) const {
  // TODO(Hideaki) implement
  std::memcpy(output_buffer, log_positions, sizeof(snapshot::BufferPosition) * log_positions_count);
  *written_count = log_positions_count;
}
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
