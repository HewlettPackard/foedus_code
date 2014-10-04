/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"

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
 * @par Branch-pages and partitions
 * \li The root of the first layer (root-of-root) is always on node-0. This is the case even
 * when the masstree consists of just one page that is updated only in node-x where x != 0.
 * In such an unlucky case, node-x needs unnecessary remote access at root, but this makes
 * composer's construct_root() simpler. Hence, if the masstree is just one page, only one
 * partition on node-0.
 * \li The same applies to all high-level pages that contain pointers to more than one partition.
 * For example, if all keys start with aaaa...aaa (repeat 80 times), then the first 10
 * layers contain just one-page respectively, all of which are in node-0.
 * Let \b branch-pages denote all kinds of these pages that are forcibly on node-0.
 * \li Usually and hopefully, there are very few branch pages. The last step of composer
 * constructs these branch pages.
 * \li A contiguous range in a branch page constitutes a partition. It might span multiple pointers
 * and in fact sometimes it's necessary (imagine the case where the branch-page is a border page
 * and the previous snapshot contained a key "h12345678". In the volatile page, it might be now
 * a pointer to next layer with slice "h1234567").
 * \li A partition does not span two branch pages. So, there is a strict hierarchy
 * branch-page -> partition -> keys
 *
 * @par Enumerating branch-pages
 * The partitioner first enumerate branch pages in a breadth-first search fashion.
 * It collects all pages of depth-n of pointer-following (whether intermediate->border
 * pointer or border->next_layer pointer). We start with n=0 and continue with n++ until we find a
 * sufficiently large number of pointers or exhaustively touch all pages if the masstree is tiny.
 * When there are \e x nodes, we continue until we find \e x*kPartitionThresholdPerNode
 * records/pointers or more. It is so far a compile-time parameter. The larger, the more granular
 * the partitioning detection. The smaller, the less the footprint/overhead of partitioner.
 *
 * @par Determining partition for each pointer
 * For each pointer from branch pages, we determine the owner node simply based on the last updater
 * of the pointed page, which is a rough statistics in the page header (no correctness guaranteed).
 * For each record in branch pages, the owner node is simply the last committer in XctId.
 * If we have contiguous pointers/records of the same owner node, we merge them to one partition.
 *
 * @par Expected issues
 * The scheme above is so simple and easy to implement/maintain.
 * Of course the simplicity has its price. If pointer-distributions are skewed, eg one pointer
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
  enum Constants {
    /**
     * We stop collecting branch pages when we find this number of records/pointers per node.
     */
    kPartitionThresholdPerNode = 8,
  };

 public:
  explicit MasstreePartitioner(Partitioner* parent);

  ErrorStack design_partition();
  bool is_partitionable() const;
  void partition_batch(
    PartitionId                     local_partition,
    const snapshot::LogBuffer&      log_buffer,
    const snapshot::BufferPosition* log_positions,
    uint32_t                        logs_count,
    PartitionId*                    results) const;

  void sort_batch(
    const snapshot::LogBuffer&        log_buffer,
    const snapshot::BufferPosition*   log_positions,
    uint32_t                          logs_count,
    const memory::AlignedMemorySlice& sort_buffer,
    Epoch                             base_epoch,
    snapshot::BufferPosition*         output_buffer,
    uint32_t*                         written_count) const;

  uint64_t  get_required_sort_buffer_size(uint32_t log_count) const;

  friend std::ostream& operator<<(std::ostream& o, const MasstreePartitioner& v);

 private:
  Engine* const               engine_;
  const StorageId             id_;
  PartitionerMetadata* const  metadata_;
  MasstreePartitionerData*    data_;
};

/**
 * @brief Dynamic information of one partitioner.
 * @details
 * Unlike array, masstree has a variable-sized partitioner data.
 * Remember that this is placed in shared memory.
 * We can't use any std::vector, std::string, etc.
 */
struct MasstreePartitionerData final {
  // only for reinterpret_cast
  MasstreePartitionerData() = delete;
  ~MasstreePartitionerData() = delete;

  // Note that you can't do sizeof(MasstreePartitionerData).
};

/**
 * A locally-allocated temporary data design_partition() constructs while designing partitions.
 * This is not placed in shared memory. Once the design is done, this is converted to
 * MasstreePartitionerData. Hence, we can use vector/string/etc in this struct.
 */
struct MasstreePartitionerInDesignData final {
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_
