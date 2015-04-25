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
#ifndef FOEDUS_STORAGE_HASH_HASH_PARTITIONER_IMPL_HPP_
#define FOEDUS_STORAGE_HASH_HASH_PARTITIONER_IMPL_HPP_

#include <stdint.h>

#include <cstring>
#include <iosfwd>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/hash_id.hpp"

namespace foedus {
namespace storage {
namespace hash {
/**
 * @brief Partitioner for a hash storage.
 * @ingroup HASH
 * @details
 * Partitioning for hash is much easier than for B-trees, but somewhat trickier than array.
 * Compared to array, there will not be and should not be any locality in terms of ranges
 * of hash bins. If there are, it means that the hash function is bad!
 *
 * @par Footprint vs Accuracy
 * The main challenge here is the tradeoff between the footprint and accuracy of the partitioning
 * information. One extreme, which is our current implementation, is to determine a node to assign
 * for every single hash bin. Assuming there are few records in each hash bin, this should give a
 * good locality so that mappers can avoid sending out too many logs to remote reducers.
 * However, we need sizeof(PartitionId) * hashbins bytes of partitioning information.
 * Another extreme is to assign partition in a super coarse granularity, such as root page pointers.
 * Then the footprint is fixed and tiny, but has basically zero accuracy. All random.
 * So far, we picked the former extreme. We thus issue an error when a user specifies a large
 * bin-bits such that 2^bits bytes >= storage_options.partitioner_data_memory_mb_ MB.
 *
 * @par Current policy and its limitations
 * Based on the above simple design, we just pick the owner of current volatile data page
 * as the partition. We don't even balance out so far. In sum, we have many limitations as follows:
 *  \li Large footprints, which might prohibit large hash-tables and cause lots of L1 misses
 * while partitioning.
 *  \li Users must make sure the initial inserts are well balanced between nodes.
 *
 * @note
 * This is a private implementation-details of \ref HASH, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class HashPartitioner final {
 public:
  explicit HashPartitioner(Partitioner* parent);

  ErrorStack design_partition(const Partitioner::DesignPartitionArguments& args);
  bool is_partitionable() const;
  void partition_batch(const Partitioner::PartitionBatchArguments& args) const;
  void sort_batch(const Partitioner::SortBatchArguments& args) const;

  const PartitionId* get_bucket_owners() const;

  friend std::ostream& operator<<(std::ostream& o, const HashPartitioner& v);

 private:
  Engine* const               engine_;
  const StorageId             id_;
  PartitionerMetadata* const  metadata_;
  HashPartitionerData*        data_;
};

struct HashPartitionerData final {
  // only for reinterpret_cast
  HashPartitionerData() = delete;
  ~HashPartitionerData() = delete;

  static uint64_t object_size(HashBin total_bin_count) {
    return total_bin_count + 16;
  }
  uint64_t object_size() const {
    return total_bin_count_ + 16;
  }

  /** if false, every record goes to node-0. single-page hash, only one SOC, etc. */
  bool                  partitionable_;   // +1 -> 1
  char                  padding_[7];      // +7 -> 8

  /** Size of the entire hash. */
  HashBin               total_bin_count_;   // +8 -> 16

  /** partition of each hash bin. Actual size is total_bin_count_ */
  PartitionId           bin_owners_[8];
};


}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_PARTITIONER_IMPL_HPP_
