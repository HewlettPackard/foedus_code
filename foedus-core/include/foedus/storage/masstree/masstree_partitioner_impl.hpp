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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_

#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <iosfwd>
#include <queue>
#include <string>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Partitioner for a masstree storage.
 * @ingroup MASSTREE
 * @details
 * The partitioner for masstree simply designs \e n pairs of a \e low-key and \e node.
 *
 * @par Low-key
 * Low-key marks the inclusive beginning of the partition with arbitrary length.
 * For example,
 * \li (low-key: zero-length string, node=0)
 * \li (low-key: "e", node=1)
 * \li (low-key: "k", node=2)
 * \li (low-key: "r", node=1)
 * \li (low-key: "w", node=2)
 *
 * This means there are 5 partitions placed in 3 nodes.
 * "abc" would be in node 0, "z" would be in node 2, etc.
 * We so far don't do KeySlice optimization used in the transactional-processing side.
 * We might do that later, but let's see if log gleaner is fast enough without fancy optimization.
 *
 * @par Design policy
 * Not suprisingly, the partition design logic is more complex than array and sequential.
 * But, we still want to make partitioner/composer as simple as possible.
 * The compromise is as follows.
 *
 * @par One slice as partition key
 * For efficiency and simplicity we always use one KeySlice (8-bytes) as low-key.
 * We initially explored arbitrary length partitiong keys, but it causes too much overhead
 * and complexity. By using one KeySlice, all comparison is just an integer comparison and
 * fixed-sized.
 *
 * @par Enumerating partitions
 * With this simplification, partition design just checks the root page of first layer.
 * If it's a border page, everything to node 0. If it's an intermediate page, we use separators
 * in it as partitions. Stupidly simple.
 * For each pointer, we determine the owner node simply based on the last updater
 * of the pointed page, which is a rough statistics in the page header (no correctness guaranteed).
 *
 * @par Expected issues
 * The scheme above is so simple and easy to implement/maintain.
 * Of course the simplicity has its price. If all keys start with a common 8-bytes, we are screwed.
 * If pointer-distributions are skewed, eg one pointer
 * leads to billion records while other pointers lead to just one record, it will result in
 * one node receiving almost all records.
 * Nevertheless, the branch-page/partition framework above is flexible enough to address the
 * issue later. We just need advanced algorithm to enumerate branch pages and determine owners.
 * Let's keep it simple for now, and work on this later.
 *
 * @note
 * This is a private implementation-details of \ref MASSTREE, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class MasstreePartitioner final {
 public:
  explicit MasstreePartitioner(Partitioner* parent);

  ErrorStack  design_partition(const Partitioner::DesignPartitionArguments& args);
  bool is_partitionable() const;
  void partition_batch(const Partitioner::PartitionBatchArguments& args) const;
  void sort_batch(const Partitioner::SortBatchArguments& args) const;

  friend std::ostream& operator<<(std::ostream& o, const MasstreePartitioner& v);

 private:
  Engine* const               engine_;
  const StorageId             id_;
  PartitionerMetadata* const  metadata_;
  MasstreePartitionerData*    data_;

  /**
   * When this is the initial snapshot. Simpler case.
   * @param[in] root a stable copy of the root volatile page. Not the actual page, which might
   * be now changing.
   */
  ErrorStack  design_partition_first(const MasstreeIntermediatePage* root);

  void sort_batch_8bytes(const Partitioner::SortBatchArguments& args) const;
  void sort_batch_general(const Partitioner::SortBatchArguments& args) const;

  /**
   * if it's a volatile page, we might be reading a half-updated image.
   * as we don't maintain read-sets like usual transactions, we instead do the optimistic
   * read protocol here.
   */
  ErrorStack read_page_safe(MasstreePage* src, MasstreePage* out);
};

/**
 * @brief Dynamic information of one partitioner.
 * @details
 * Because partition keys are KeySlice, this is fixed-size like ArrayPartitionerData.
 */
struct MasstreePartitionerData final {
  // only for reinterpret_cast
  MasstreePartitionerData() = delete;
  ~MasstreePartitionerData() = delete;

  /** Returns the partition (node ID) that should contain the key */
  uint16_t find_partition(const char* key, uint16_t key_length) const;

  uint16_t    partition_count_;
  KeySlice    low_keys_[kMaxIntermediatePointers];
  uint16_t    partitions_[kMaxIntermediatePointers];
};

/**
 * Number of vol pages in each node sampled per pointer in the root page.
 * Will be probably used in other partitioners, too.
 */
struct OwnerSamples {
  OwnerSamples(uint32_t nodes, uint32_t subtrees) : nodes_(nodes), subtrees_(subtrees) {
    occurrences_ = new uint32_t[nodes * subtrees];
    std::memset(occurrences_, 0, sizeof(uint32_t) * nodes * subtrees);
    assignments_ = new uint32_t[subtrees];
    std::memset(assignments_, 0, sizeof(uint32_t) * subtrees);
  }
  ~OwnerSamples() {
    delete[] occurrences_;
    occurrences_ = nullptr;
    delete[] assignments_;
    assignments_ = nullptr;
  }

  /** number of nodes */
  const uint32_t  nodes_;
  /** number of pointers to children in the root page */
  const uint32_t  subtrees_;
  /** number of occurences of a volatile page in the node. @see at() */
  uint32_t*       occurrences_;
  /** node_id to be the owner of the subtree */
  uint32_t*       assignments_;

  void increment(uint32_t node, uint32_t subtree_id) {
    ASSERT_ND(node < nodes_);
    ASSERT_ND(subtree_id < subtrees_);
    ++occurrences_[subtree_id * nodes_ + node];
  }
  uint32_t at(uint32_t node, uint32_t subtree_id) const {
    ASSERT_ND(node < nodes_);
    ASSERT_ND(subtree_id < subtrees_);
    return occurrences_[subtree_id * nodes_ + node];
  }

  uint32_t get_assignment(uint32_t subtree_id) const { return assignments_[subtree_id]; }

  /** Determine assignments based on the samples */
  void assign_owners();

  friend std::ostream& operator<<(std::ostream& o, const OwnerSamples& v);
};

////////////////////////////////////////////////////////////////////////////////
///
///      Local utility functions. Used in partitioner and composer.
///
////////////////////////////////////////////////////////////////////////////////
/** Returns negative, 0, positive if left<right, left==right, left>right. */
inline int compare_keys(
  const char* left,
  uint16_t left_length,
  const char* right,
  uint16_t right_length) {
  uint16_t cmp_length = std::min(left_length, right_length);
  int result = std::memcmp(left, right, cmp_length);
  if (result != 0) {
    return result;
  } else if (left_length < right_length) {
    return -1;
  } else if (left_length > right_length) {
    return 1;
  } else {
    return 0;
  }
}

inline const MasstreeCommonLogType* resolve_log(
  const snapshot::LogBuffer& log_buffer,
  snapshot::BufferPosition pos) {
  const MasstreeCommonLogType* rec = reinterpret_cast<const MasstreeCommonLogType*>(
    log_buffer.resolve(pos));
  ASSERT_ND(rec->header_.get_type() == log::kLogCodeMasstreeInsert
    || rec->header_.get_type() == log::kLogCodeMasstreeDelete
    || rec->header_.get_type() == log::kLogCodeMasstreeOverwrite);
  return rec;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_
