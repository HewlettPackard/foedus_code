/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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

ErrorStack SequentialPartitioner::design_partition(
  const Partitioner::DesignPartitionArguments& /*args*/) {
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
