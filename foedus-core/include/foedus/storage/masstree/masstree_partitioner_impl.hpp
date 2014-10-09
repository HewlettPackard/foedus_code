/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_

#include <stdint.h>

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
 * pointers or more. It is so far a compile-time parameter. The larger, the more granular
 * the partitioning detection. The smaller, the less the footprint/overhead of partitioner.
 *
 * @par Determining partition for each pointer
 * For each pointer from branch pages, we determine the owner node simply based on the last updater
 * of the pointed page, which is a rough statistics in the page header (no correctness guaranteed).
 * We assume all records (not pointers to next layer) in branch pages are implicitly owned by
 * the previous pointer. In other words, we ignore records while designing partitions.
 * If we have contiguous pointers of the same owner node, we merge them to one partition.
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
 public:
  enum Constants {
    /**
     * We stop collecting branch pages when we find this number of pointers per node.
     */
    kPartitionThresholdPerNode = 8,
  };

  explicit MasstreePartitioner(Partitioner* parent);

  ErrorStack  design_partition(
    memory::AlignedMemory* work_memory,
    cache::SnapshotFileSet* snapshot_files);
  uint64_t    get_required_design_buffer_size() const;

  bool is_partitionable() const;
  void partition_batch(const Partitioner::PartitionBatchArguments& args) const;
  void sort_batch(const Partitioner::SortBatchArguments& args) const;

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
  struct PartitionHeader {
    /** byte offset of the low-key relative to get_key_region() */
    uint32_t  key_offset_;
    /** byte length of the low-key */
    uint16_t  key_length_;
    /** partition owner */
    uint16_t  node_;
  };

  // only for reinterpret_cast
  MasstreePartitionerData() = delete;
  ~MasstreePartitionerData() = delete;

  char* get_key_region() { return reinterpret_cast<char*>(partitions_ + partition_count_); }
  const char* get_key_region() const {
    return reinterpret_cast<const char*>(partitions_ + partition_count_);
  }
  const char* get_partition_key(uint16_t partition) const {
    return get_key_region() + partitions_[partition].key_offset_;
  }

  /** Returns the partition (node ID) that should contain the key */
  uint16_t find_partition(const char* key, uint16_t key_length) const;

  // Note that you can't do sizeof(MasstreePartitionerData).

  uint16_t  partition_count_;
  char      padding_[6];
  /**
   * this is actually of size partition_count_. further partitions_[partition_count_] and later
   * are used to store low-key.
   */
  PartitionHeader partitions_[1];
};

/**
 * A locally-allocated temporary data design_partition() constructs while designing partitions.
 * This is not placed in shared memory. Once the design is done, this is converted to
 * MasstreePartitionerData. Hence, we can use vector/string/etc in this struct.
 */
struct MasstreePartitionerInDesignData final {
  /**
   * Represents a branch page.
   */
  struct BranchPage {
    BranchPage() {}
    BranchPage(
      const std::string& prefix,
      memory::PagePoolOffset tmp_page_offset,
      uint16_t owner_node)
      : prefix_(prefix), tmp_page_offset_(tmp_page_offset), owner_node_(owner_node) {}
    /*
     * If the page is layer-1 or deeper, this stores the 8*layer bytes prefix.
     */
    std::string             prefix_;

    /**
     * Only for branch page. offset in tmp_pages_ that points to a copy of this page.
     * 0 if record_.
     */
    memory::PagePoolOffset  tmp_page_offset_;

    /** NUMA node that seems to own this page. this is not a final decision. */
    uint16_t                owner_node_;
  };
  /** Finalized partition info */
  struct Partition {
    Partition() {}
    Partition(const std::string& low_key, uint16_t owner_node)
      : low_key_(low_key), owner_node_(owner_node) {}
    std::string low_key_;
    uint16_t    owner_node_;
  };

  MasstreePartitionerInDesignData(
    Engine* engine,
    StorageId id,
    memory::AlignedMemory* work_memory,
    cache::SnapshotFileSet* snapshot_files);
  ~MasstreePartitionerInDesignData();

  ErrorStack  initialize();

  /** Phase-1. Enumerate enough branch pages as inputs */
  ErrorStack  enumerate();

  /** Phase-2. Finalize partition in designed_partitions_ based on enumerated info. */
  ErrorStack  design();

  /**
   * Phase-3. Last step. Copy the designed partition information to a shared memory.
   */
  void        copy_output(MasstreePartitionerData* destination) const;
  /** Caller must reserve this size before invoking copy_output */
  uint32_t    get_output_size() const;

  /** Follows a volatile page pointer. If there isn't, follows snapshot page. */
  ErrorStack  read_page(const DualPagePointer& ptr, memory::PagePoolOffset offset);
  /** Consume a branch page at the beginning of enumerated_pages_ and add pointers/records in it */
  ErrorStack  pop_and_explore_branch_page();
  /** Adds the given pointer to enumerated_pages_ */
  ErrorStack  push_branch_page(const DualPagePointer& ptr, const std::string& prefix);

  MasstreePage* resolve_tmp_page(memory::PagePoolOffset offset);

  /**
   * When this becomes desired_branches_ or larger, we stop enumeration.
   */
  uint32_t      get_branch_count() const {
    return active_pages_.size() + terminal_pages_.size();
  }

  Engine* const           engine_;
  const StorageId         id_;
  const MasstreeStorage   storage_;
  /** Memory for tmp_pages_. */
  memory::AlignedMemory* const  work_memory_;
  /** To read from snapshot pages. */
  cache::SnapshotFileSet* const snapshot_files_;
  const uint32_t          desired_branches_;
  const memory::GlobalVolatilePageResolver& volatile_resolver_;

  // above are const, below are dynamic

  /**
   * All branch pages to be explored, in the order of enumeration.
   * Newly enumerated pages are appended at last, and the earliest-enumerated page
   * is dequeued from the top to spawn pages pointed from it. So, it's a FIFO queue.
   */
  std::queue<BranchPage>    active_pages_;
  /**
   * All branch pages that were already checked out to find pointers, but didn't have any pointers.
   * This means it's a boundary page with only records, from which we can't digg down further.
   * We keep them in a separate vector.
   */
  std::vector<BranchPage>   terminal_pages_;

  /** design() populates this as a result */
  std::vector<Partition>    designed_partitions_;

  /** Actually of PagePoolControlBlock. */
  char                      tmp_pages_control_block_[128];  // so far this is more than enough
  /**
   * Everything design_partition() reads is a copied image of pages stored here.
   * It's a small page pool just enough for desired_branches_ plus a few more pages (in case
   * the last ).
   */
  memory::PagePool          tmp_pages_;
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PARTITIONER_IMPL_HPP_
