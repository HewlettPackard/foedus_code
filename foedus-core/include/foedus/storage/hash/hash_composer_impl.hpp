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

  HashDataPage* resolve_volatile(VolatilePagePointer pointer);
  Composer::DropResult drop_volatiles_recurse(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer);
  /** also returns if we kept the volatile data pages */
  Composer::DropResult drop_volatiles_intermediate(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer,
    HashIntermediatePage* volatile_page);
  Composer::DropResult drop_volatiles_bin(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer,
    HashDataPage* volatile_page);
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
 * @par Exception for 1-level hashtable
 * If the hash table has only one level (root directly points to data pages), then
 * HashRootInfoPage also directly points to HashDataPage. No HashComposedBinsPage created.
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
  ErrorStack initialize(HashBin initial_bin);
  ErrorStack init_root_page();

  /**
   * apply the range of logs in a tight loop.
   * this needs to access the logs themselves, which might cause L1 cache miss.
   * the fetch method ameriolates it by pararell prefetching for this number.
   */
  void apply_batch(uint64_t cur, uint64_t next);

  /**
   * separate and trivial implementation of execute() for when the hash has only the root
   * intermediate page, which directly points to data pages.
   * All other methods assume that the root page points to child interemediate pages.
   */
  ErrorStack execute_single_level_hash();
  ErrorStack finalize();

  HashIntermediatePage* get_cur_path(uint8_t level) const { return cur_path_ + level; }
  /** @returns The bin range of cur_path_[0]. */
  const HashBinRange&   get_cur_range_level0() const ALWAYS_INLINE {
    return cur_path_[0].get_bin_range();
  }
  ErrorCode init_cur_path();
  ErrorCode update_cur_path(HashBin bin);

  ErrorCode read_or_init_page(
    SnapshotPagePointer old_page_id,
    SnapshotPagePointer new_page_id,
    HashBin bin,
    HashDataPage* bin_head) ALWAYS_INLINE;

  /** call this before obtaining a new intermediate page */
  ErrorCode expand_intermediate_pool_if_needed() ALWAYS_INLINE;

  /**
   * Called at the end of execute() to install pointers to snapshot pages constructed in this
   * composer. The snapshot pointer to intermediate pages is separately installed later.
   * This method does not drop volatile pages, it just installs snapshot pages to data pages,
   * thus it's trivially safe as far as the snapshot pages are already flushed to the storage.
   */
  ErrorStack install_snapshot_pointers(
    SnapshotPagePointer snapshot_base,
    uint64_t* installed_count) const;

  /** dump everything in main buffer (intermediate pages are kept) */
  ErrorCode dump_data_pages();
  /** used only in debug mode */
  bool verify_cur_path() const;
  bool verify_snapshot_pointer(storage::SnapshotPagePointer pointer);

  bool is_initial_snapshot() const { return previous_root_page_pointer_ == 0; }


  uint16_t get_root_children() const;

  // these properties are initialized in constructor and never changed afterwards
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
  uint16_t                        padding_;
  const HashBin                   total_bin_count_;
  const SnapshotPagePointer       previous_root_page_pointer_;

  /**
   * cur_path_[n] is the snapshot intermediate page of level-n.
   * We only read from them. We don't modify them.
   */
  HashIntermediatePage*           cur_path_;
  /**
   * A small memory to back cur_path_.
   */
  memory::AlignedMemory           cur_path_memory_;

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
   * In a 1-level hash, no intermediate pages created.
   * Otherwise, this starts with root_children_ HashComposedBinsPage, and those pages remain
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
