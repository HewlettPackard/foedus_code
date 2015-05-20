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
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_COMPOSER_IMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_COMPOSER_IMPL_HPP_

#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_route.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Composer for an array storage.
 * @ingroup ARRAY
 * @details
 *
 * @note
 * This is a private implementation-details of \ref ARRAY, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class ArrayComposer final {
 public:
  explicit ArrayComposer(Composer *parent);

  std::string to_string() const;

  ErrorStack compose(const Composer::ComposeArguments& args);
  ErrorStack construct_root(const Composer::ConstructRootArguments& args);
  Composer::DropResult  drop_volatiles(const Composer::DropVolatilesArguments& args);
  void                  drop_root_volatile(const Composer::DropVolatilesArguments& args);

 private:
  Engine* const             engine_;
  const StorageId           storage_id_;
  const ArrayStorage        storage_;

  ArrayPage*  resolve_volatile(VolatilePagePointer pointer);
  Composer::DropResult drop_volatiles_recurse(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer);
  /** also returns if we kept the volatile leaf page */
  Composer::DropResult drop_volatiles_intermediate(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer,
    ArrayPage* volatile_page);
  Composer::DropResult drop_volatiles_leaf(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer,
    ArrayPage* volatile_page);
  bool is_to_keep_volatile(uint16_t level);
  /** Used only from drop_root_volatile. Drop every volatile page. */
  void drop_all_recurse(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer);
};

/**
 * Output of one compose() call, which are then combined in construct_root().
 * If the root page is leaf page (single-page array), this contains just one pointer to the root.
 * If not, this contains pointers to direct children of root.
 */
struct ArrayRootInfoPage final {
  PageHeader          header_;

  /** Pointers to direct children of root. 0 if not set in this compose() */
  SnapshotPagePointer pointers_[kInteriorFanout];

  char                filler_[
    kPageSize
    - sizeof(PageHeader)
    - kInteriorFanout * sizeof(SnapshotPagePointer)];
};

/**
 * ArrayComposer's compose() implementation separated from the class itself.
 * It's a complicated method, so worth being its own class.
 * This defines all the variables maintained during one compose() call.
 */
class ArrayComposeContext {
 public:
  ArrayComposeContext(
    Engine*                           engine,
    snapshot::MergeSort*              merge_sort,
    snapshot::SnapshotWriter*         snapshot_writer,
    cache::SnapshotFileSet*           previous_snapshot_files,
    Page*                             root_info_page);

  ErrorStack execute();

 private:
  ErrorStack initialize(ArrayOffset initial_offset);
  ErrorStack init_root_page();

  /**
   * apply the range of logs in a tight loop.
   * this needs to access the logs themselves, which might cause L1 cache miss.
   * the fetch method ameriolates it by pararell prefetching for this number.
   */
  void apply_batch(uint64_t cur, uint64_t next);

  /**
   * separate and trivial implementation of execute() for when the array has only one page.
   * All other methods assume that the root page is an interemediate page.
   */
  ErrorStack execute_single_level_array();
  ErrorStack finalize();

  ErrorCode update_cur_path(ArrayOffset next_offset);

  ErrorCode read_or_init_page(
    SnapshotPagePointer old_page_id,
    SnapshotPagePointer new_page_id,
    uint8_t level,
    ArrayRange range,
    ArrayPage* page) ALWAYS_INLINE;

  /**
   * creates empty snapshot pages that didn't receive any logs during the initial snapshot.
   * We have to create snapshot pages even for such pages only for initial snapshot.
   */
  ErrorCode create_empty_pages(ArrayOffset from, ArrayOffset to);
  ErrorCode create_empty_pages_recurse(ArrayOffset from, ArrayOffset to, ArrayPage* page);
  ErrorCode create_empty_intermediate_page(ArrayPage* parent, uint16_t index, ArrayRange range);
  ErrorCode create_empty_leaf_page(ArrayPage* parent, uint16_t index, ArrayRange range);

  /** call this before obtaining a new intermediate page */
  ErrorCode expand_intermediate_pool_if_needed() ALWAYS_INLINE;

  /**
   * Called at the end of execute() to install pointers to snapshot pages constructed in this
   * composer. The snapshot pointer to the combined root is separately installed later.
   * This method does not drop volatile pages, it just installs snapshot pages, thus it's
   * trivially safe as far as the snapshot pages are already flushed to the storage.
   */
  ErrorStack install_snapshot_pointers(
    SnapshotPagePointer snapshot_base,
    uint64_t* installed_count) const;
  ErrorCode install_snapshot_pointers_recurse(
    SnapshotPagePointer snapshot_base,
    const memory::GlobalVolatilePageResolver& resolver,
    const ArrayPage* snapshot_page,
    ArrayPage* volatile_page,
    uint64_t* installed_count) const;

  /** dump everything in main buffer (intermediate pages are kept) */
  ErrorCode dump_leaf_pages();
  /** used only in debug mode */
  bool verify_cur_path() const;
  bool verify_snapshot_pointer(storage::SnapshotPagePointer pointer);

  /** returns page range of a leaf page containing the offset */
  ArrayRange to_leaf_range(ArrayOffset offset) const {
    ArrayOffset begin = offset / offset_intervals_[0] * offset_intervals_[0];
    return ArrayRange(begin, begin + offset_intervals_[0], storage_.get_array_size());
  }
  bool is_initial_snapshot() const { return previous_root_page_pointer_ == 0; }

  uint16_t get_root_children() const;

  // these properties are initialized in constructor and never changed afterwards
  Engine* const                   engine_;
  snapshot::MergeSort* const      merge_sort_;
  const Epoch                     system_initial_epoch_;
  const StorageId                 storage_id_;
  const snapshot::SnapshotId      snapshot_id_;
  const ArrayStorage              storage_;
  snapshot::SnapshotWriter* const snapshot_writer_;
  cache::SnapshotFileSet*  const  previous_snapshot_files_;
  /** The final output of the compose() call */
  ArrayRootInfoPage* const        root_info_page_;

  const uint16_t                  payload_size_;
  const uint8_t                   levels_;
  const SnapshotPagePointer       previous_root_page_pointer_;

  /**
   * The offset interval a single page represents in each level. index=level.
   * So, offset_intervals[0] is the number of records in a leaf page.
   */
  uint64_t                        offset_intervals_[kMaxLevels];

  /**
  * cur_path_[0] points to leaf we are now modifying, cur_path_[1] points to its parent.
  * cur_path_[levels_-1] is always the root page, of course.
  * cur_path_[levels_-1] is null only when no log is processed yet.
  */
  ArrayPage*                cur_path_[kMaxLevels];

  /**
  * How many pages we allocated in the main buffer of snapshot_writer.
  * This is reset to zero for each buffer flush.
  */
  uint32_t                  allocated_pages_;
  /**
  * How many pages we allocated in the intermediate-page buffer of snapshot_writer.
  * This is purely monotonially increasing beecause we keep intermediate pages until the end
  * of compose().
  * The first intermediate page (relative index=0) is always the root page.
  */
  uint32_t                  allocated_intermediates_;
  uint32_t                  max_pages_;
  uint32_t                  max_intermediates_;
  /** same as snapshot_writer_->get_page_base()*/
  ArrayPage*                page_base_;
  /** same as snapshot_writer_->get_intermediate_base()*/
  ArrayPage*                intermediate_base_;

  /**
   * we need partitioning information just for the initial filling.
   * this is used only when there is a range of pages that received no logs so that we have to
   * create empty pages in this partition.
   */
  const ArrayPartitionerData* partitioning_data_;
};

static_assert(sizeof(ArrayRootInfoPage) == kPageSize, "incorrect sizeof(RootInfoPage)");
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_COMPOSER_IMPL_HPP_
