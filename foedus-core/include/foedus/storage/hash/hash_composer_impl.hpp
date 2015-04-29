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
#ifndef FOEDUS_STORAGE_HASH_HASH_COMPOSER_IMPL_HPP_
#define FOEDUS_STORAGE_HASH_HASH_COMPOSER_IMPL_HPP_

#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/hash/hash_tmpbin.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace hash {
/**
 * @brief Composer for a hash storage.
 * @ingroup HASH
 * @details
 * Composer for hash is much easier than for B-trees, but somewhat trickier than array.
 * Unlike B-trees, we don't have to worry about page-splits, finding where to insert,
 * etc etc. We re-construct each hash bin always as a whole. Huuuuge simplification.
 * Unlike array, we have a quite granular partitioning, so constructing intermediate pages is
 * trickier. We do 2-path construction to parallelize this part without losing partitioning benefit.
 *
 * @section HASH_COMPOSE_RESULTS HashRootInfoPage as the results
 * @copydetails foedus::storage::hash::HashRootInfoPage
 *
 * @note
 * This is a private implementation-details of \ref HASH, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class HashComposer final {
 public:
  explicit HashComposer(Composer *parent);

  std::string to_string() const;

  ErrorStack compose(const Composer::ComposeArguments& args);
  ErrorStack construct_root(const Composer::ConstructRootArguments& args);


  Composer::DropResult  drop_volatiles(const Composer::DropVolatilesArguments& args);
  void                  drop_root_volatile(const Composer::DropVolatilesArguments& args);

 private:
  Engine* const             engine_;
  const StorageId           storage_id_;
  const HashStorage         storage_;

  /** just because we use it frequently... */
  const memory::GlobalVolatilePageResolver& volatile_resolver_;

  HashDataPage* resolve_data(VolatilePagePointer pointer) const ALWAYS_INLINE;
  HashIntermediatePage* resolve_intermediate(VolatilePagePointer pointer) const ALWAYS_INLINE;

  Composer::DropResult drop_volatiles_recurse(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer);

  /**
   * @returns if the given bin data pages contain any information later than the given
   * threshold epoch (valid_until).
   */
  bool can_drop_volatile_bin(VolatilePagePointer head, Epoch valid_until) const;

  /** Drops all data pages in a bin, starting from the pointer. */
  void drop_volatile_entire_bin(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer_to_head) const;

  bool is_to_keep_volatile(uint16_t level);
  /** Used only from drop_root_volatile. Drop every volatile page. */
  void drop_all_recurse(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer);
};

struct ComposedBin {
  HashBin             bin_;
  SnapshotPagePointer page_id_;
};

const uint16_t kHashComposedBinsPageMaxBins = (kPageSize - 64) / sizeof(ComposedBin);

/**
 * Output of compose() call consists of many of this pages.
 * Each HashComposedBinsPage contains lots of snapshotted-bins.
 * This is simply a linked list connected by the next-page pointer.
 * All bins stored here are sorted by bin.
 */
struct HashComposedBinsPage final {
  PageHeader          header_;      // +32 -> 32
  HashBinRange        bin_range_;   // +16 -> 48
  SnapshotPagePointer next_page_;   // +8  -> 56
  uint32_t            bin_count_;   // +4  -> 60
  uint32_t            dummy_;       // +4  -> 64
  ComposedBin         bins_[kHashComposedBinsPageMaxBins];    // -> 4096
};

/**
 * @brief Output of one compose() call, which are then combined in construct_root().
 * @details
 * The format is exactly same as HashIntermediatePage, but typedef-ed for clarity. The only
 * difference is it points to HashComposedBinsPage instead of HashDataPage/HashIntermediatePage.
 *
 * @par Linked list of HashComposedBinsPage
 * In hash storage, what compose() returns are just a list of bins.
 * There essentially is no structure of them.
 * The only reason we have this "root" page is to allow parallel combination by the gleaner.
 * Each child-pointer from the root page is assigned to one thread on its own node,
 * then written out in that node. So, after following the first child pointer, we just
 * need a linked list, which is HashComposedBinsPage.
 *
 * @par 2-path composition
 * Each reducer returns a linked-list of HashComposedBinsPage for every direct-child of the
 * root page. Then, the gleaner combines them in a parallel fashion, resulting in snapshot
 * interemediate pages that will be installed to the root page.
 *
 * @par 1-level hashtable
 * It works the same way even when the hash table has only one level (root directly points to data
 * pages). Even in this case, HashRootInfoPage still points to HashComposedBinsPage,
 * which contains only one entry per page. Kind of wasteful, but it has negligible overheads
 * compared to the entire mapper/reducer.
 * Unlike array package, we don't do special optimization for this case in order to keep the code
 * simpler, and because the storage could benefit from parallelization even in this case
 * (the root page still contains hundreds of hash bins).
 */
typedef HashIntermediatePage HashRootInfoPage;

/**
 * HashComposer's compose() implementation separated from the class itself.
 * It's a complicated method, so worth being its own class.
 * This defines all the variables maintained during one compose() call.
 */
class HashComposeContext {
 public:
  HashComposeContext(
    Engine*                           engine,
    snapshot::MergeSort*              merge_sort,
    snapshot::SnapshotWriter*         snapshot_writer,
    cache::SnapshotFileSet*           previous_snapshot_files,
    Page*                             root_info_page);

  ErrorStack execute();

 private:
  /**
   * Apply the range of logs that are all of cur_bin_ in a tight loop.
   * Well, the implementation of this method is not so batched so far, but it can be later.
   */
  ErrorCode apply_batch(uint64_t cur, uint64_t next);

  ErrorStack finalize();

  /** dump everything in main buffer (intermediate pages are kept) */
  ErrorCode dump_data_pages();

  /** @returns if the given pointer is valid as a snapshot page ID in this new snapshot */
  bool verify_new_pointer(storage::SnapshotPagePointer pointer) const;
  /** @returns if the given pointer is null or valid as a snapshot page ID in previous snapshots */
  bool verify_old_pointer(storage::SnapshotPagePointer pointer) const;

  HashDataPage* resolve_data(VolatilePagePointer pointer) const ALWAYS_INLINE;
  HashIntermediatePage* resolve_intermediate(VolatilePagePointer pointer) const ALWAYS_INLINE;

  bool is_initial_snapshot() const { return previous_root_page_pointer_ == 0; }

  ///////////////////////////////////////////////////////////////
  //// cur_path_ related methods
  ///////////////////////////////////////////////////////////////
  /** @return intermediate page of the given level in cur_path */
  HashIntermediatePage*   get_cur_path_page(uint8_t level) const { return cur_path_ + level; }
  /** @return intermediate page of the lowest valid level in cur_path */
  HashIntermediatePage*   get_cur_path_lowest() const {
    return get_cur_path_page(cur_path_lowest_level_);
  }
  /**
   * @return page ID of a head-data page of the given bin in previous snapshot. 0 (null) if
   * the data page doesn't exist in previous snapshot.
   * @pre cur_path_valid_range_.contains(bin)
   */
  SnapshotPagePointer     get_cur_path_bin_head(HashBin bin) const;

  /** Initialize the cur_path_ to bin-0. */
  ErrorStack              init_cur_path();
  /**
   * Move the cur_path so that it contains the given bin.
   * @post cur_path_valid_range_.contains(bin), \b BUT cur_path_lowest_level_ might be not 0.
   */
  ErrorCode               update_cur_path(HashBin bin);
  /**
   * Invokes update_cur_path() when needed.
   * It does nothing if is_initial_snapshot() or
   * cur_path_valid_range_.contains(bin) && cur_path_lowest_level_ == 0.
   * This method is inlined to allow the check-part efficient while the switch part is not.
   */
  ErrorCode               update_cur_path_if_needed(HashBin bin) ALWAYS_INLINE;
  /** used only in debug mode */
  bool                    verify_cur_path() const;

  ///////////////////////////////////////////////////////////////
  //// cur_bin related methods
  ///////////////////////////////////////////////////////////////
  /**
   * Finalizes the content of cur_bin_table_ and write it out as data pages.
   * @post cur_bin_ == kCurBinNotOpened
   */
  ErrorStack              close_cur_bin();
  /**
   * Loads data pages in previous snapshot and initializes cur_bin_table_ with the existing records.
   * @pre cur_bin_ == kCurBinNotOpened
   * @post cur_bin_ == bin
   */
  ErrorStack              open_cur_bin(HashBin bin);

  ///////////////////////////////////////////////////////////////
  //// HashComposedBinsPage (intermediate) related methods
  ///////////////////////////////////////////////////////////////
  /** Prepares empty HashComposedBinsPage for all direct children in the root page */
  ErrorStack              init_intermediates();
  /** @returns the head of linked-list for each direct child in the root page. */
  HashComposedBinsPage*   get_intermediate_head(uint8_t root_index) const {
    ASSERT_ND(root_index < storage_.get_root_children());
    return intermediate_base_ + root_index;
  }
  /** @returns the tail of linked-list for each direct child in the root page. */
  HashComposedBinsPage*   get_intermediate_tail(uint8_t root_index) const;

  /**
   * Switch cur_intermediate_tail_ for the given bin.
   * @pre !cur_intermediate_tail_->bin_range_.contains(bin)
   * @post cur_intermediate_tail_->bin_range_.contains(bin)
   */
  void                    update_cur_intermediate_tail(HashBin bin);
  /** Invokes update_cur_intermediate_tail if needed. Inlined to do checks efficiently */
  void                    update_cur_intermediate_tail_if_needed(HashBin bin) ALWAYS_INLINE;

  /**
   * Adds the pointer to finalized data pages of one hash bin to cur_intermediate_tail_.
   * @pre cur_intermediate_tail_->bin_range_.contains(bin)
   * @param[in] page_id The \e finalized snapshot page ID of head page of the given bin
   * @param[in] bin The hash bin that has been finalized.
   */
  ErrorCode               append_to_intermediate(SnapshotPagePointer page_id, HashBin bin);

  /** call this before obtaining a new intermediate page */
  ErrorCode               expand_intermediate_pool_if_needed();

  /**
   * Called at the end of execute() to install pointers to snapshot \e data pages constructed in
   * this composer. The snapshot pointer to intermediate pages is separately installed later.
   * This method does not drop volatile pages, it just installs snapshot pages to data pages,
   * thus it's trivially safe as far as the snapshot data pages are already flushed to the storage.
   */
  ErrorStack install_snapshot_data_pages(uint64_t* installed_count) const;
  /** Subroutine of install_snapshot_pointers_data_pages() called for each direct-child of root */
  ErrorStack install_snapshot_data_pages_root_child(
    const HashComposedBinsPage* composed,
    HashIntermediatePage* volatile_root_child,
    uint64_t* installed_count) const;

  ///////////////////////////////////////////////////////////////
  //// immutable properties
  ///////////////////////////////////////////////////////////////
  Engine* const                   engine_;
  snapshot::MergeSort* const      merge_sort_;
  const Epoch                     system_initial_epoch_;
  const StorageId                 storage_id_;
  const snapshot::SnapshotId      snapshot_id_;
  const HashStorage               storage_;
  snapshot::SnapshotWriter* const snapshot_writer_;
  cache::SnapshotFileSet*  const  previous_snapshot_files_;
  /** The final output of the compose() call */
  HashRootInfoPage* const         root_info_page_;

  const bool                      partitionable_;
  const uint8_t                   levels_;
  const uint8_t                   bin_bits_;
  const uint8_t                   bin_shifts_;
  const uint16_t                  root_children_;
  const uint16_t                  numa_node_;
  const HashBin                   total_bin_count_;
  const SnapshotPagePointer       previous_root_page_pointer_;

  /** just because we use it frequently... */
  const memory::GlobalVolatilePageResolver& volatile_resolver_;

  ///////////////////////////////////////////////////////////////
  //// mutable properties
  ///////////////////////////////////////////////////////////////
  /**
   * cur_path_[n] is the snapshot intermediate page of level-n.
   * We only read from them. We don't modify them.
   * @see cur_path_valid_upto_
   */
  HashIntermediatePage*           cur_path_;
  /**
   * A small memory to back cur_path_.
   */
  memory::AlignedMemory           cur_path_memory_;

  /**
   * cur_path_[n] is invalid where n is less than this value.
   * In other words, cur_path_[n] is non-existent in previous snapshot when that is the case.
   * If this value is levels_, even root (levels_ - 1) is invalid, meaning no previous snapshot.
   */
  uint8_t                         cur_path_lowest_level_;

  /**
   * The bin range of cur_path_[cur_path_lowest_level_].
   * If cur_path_lowest_level_ == levels_, (0,0), but this value shouldn't be used in that case.
   */
  HashBinRange                    cur_path_valid_range_;

  /**
   * The hash bin that is currently composed.
   * Whenever we observe a log whose bin is different from this value, we close the current bin
   * and write out data pages.
   * When no bin is being composed, kCurBinNotOpened.
   */
  HashBin                         cur_bin_;
  const HashBin                   kCurBinNotOpened = (1ULL << kHashMaxBinBits);

  /**
   * Small hashtable of records being modified in cur_bin_.
   */
  HashTmpBin                      cur_bin_table_;

  /** Just memory of one-page to read data pages in previous snapshot */
  memory::AlignedMemory           data_page_io_memory_;

  /**
   * Points to the HashComposedBinsPage to which we will add cur_bin_ data pages when they are done.
   * This is the tail of the linked-list for the sub-tree. When this page becomes full, we append
   * next pages and move on.
   */
  HashComposedBinsPage*           cur_intermediate_tail_;

  /**
   * How many pages we allocated in the main buffer of snapshot_writer.
   * This is reset to zero for each buffer flush.
   */
  uint32_t                  allocated_pages_;
  /**
   * How many pages we allocated in the intermediate-page buffer of snapshot_writer.
   * This is purely monotonially increasing beecause we keep intermediate pages until the end
   * of compose().
   */
  uint32_t                  allocated_intermediates_;
  uint32_t                  max_pages_;
  uint32_t                  max_intermediates_;
  /** same as snapshot_writer_->get_page_base()*/
  HashDataPage*             page_base_;
  /**
   * same as snapshot_writer_->get_intermediate_base().
   * This starts with root_children_ HashComposedBinsPage, and those pages remain
   * as the heads of HashComposedBinsPage linked-lists.
   */
  HashComposedBinsPage*     intermediate_base_;
};

static_assert(sizeof(HashRootInfoPage) == kPageSize, "incorrect sizeof(RootInfoPage)");
static_assert(sizeof(HashComposedBinsPage) == kPageSize, "incorrect sizeof(HashComposedBinsPage)");


}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_COMPOSER_IMPL_HPP_
