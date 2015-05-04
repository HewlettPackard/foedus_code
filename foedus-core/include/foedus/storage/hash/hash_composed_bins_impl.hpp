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
#ifndef FOEDUS_STORAGE_HASH_HASH_COMPOSED_BINS_IMPL_HPP_
#define FOEDUS_STORAGE_HASH_HASH_COMPOSED_BINS_IMPL_HPP_

#include <stdint.h>

#include <memory>

#include "foedus/compiler.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/cache/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"

namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief Output of one compose() call, which are then combined in construct_root().
 * @ingroup HASH
 * @details
 * The format is exactly same as HashIntermediatePage, but typedef-ed for clarity. The only
 * differences are:
 * \li Snapshot pointer points to HashComposedBinsPage instead of HashDataPage/HashIntermediatePage.
 * \li Volatile pointer (which of course is never used in snapshot page) instead represents the
 * number of contiguously-written HashComposedBinsPage in the pointed sub-tree. This allows the
 * combiner to read the entire linked-list in one-shot.
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
 * The second phase consists of reading the HashComposedBinsPage from each composer (reducer),
 * collecting updated bins on each page, and creating a new interemediate page hierarchically.
 * We have several classes decoupled from the composer class itself as listed below:
 * \li ComposedBin (individual bin)
 * \li HashComposedBinsPage  (hundreds of bins)
 * \li ComposedBinsBuffer (all bins from a sub-tree emit from one composer)
 * \li ComposedBinsMergedStream (all bins from a sub-tree emit from all composers)
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
 * @brief Represents an output of composer on one bin.
 * @ingroup HASH
 */
struct ComposedBin final {
  HashBin             bin_;
  SnapshotPagePointer page_id_;
};

const uint16_t kHashComposedBinsPageMaxBins = (kPageSize - 64) / sizeof(ComposedBin);

/**
 * @brief A page to pack many ComposedBin as an output of composer.
 * @ingroup HASH
 * @details
 * Output of a composer consists of many of this pages.
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
 * @brief Abstracts how we batch-read several HashComposedBinsPage emit from individual composers.
 * @ingroup HASH
 * @details
 * This buffer is instantiated for each child of each HashRootInfoPage, which has several
 * HashComposedBinsPage contiguously. We batch-read them to reduce I/O.
 */
struct ComposedBinsBuffer final {
  enum Constants {
    kMinBufferSize = 1 << 6,
  };
  /** file handles */
  cache::SnapshotFileSet* fileset_;
  /** Page ID of the head of HashComposedBinsPage for this sub-tree. */
  SnapshotPagePointer   head_page_id_;
  /** Number of HashComposedBinsPage from head_page_id_ contiguously emit by a composer */
  uint32_t              total_pages_;
  /** How many pages buffer_ can hold */
  uint32_t              buffer_size_;
  /**
   * index (0=head, total_pages_ - 1=tail, ) of the first page in the buffer_.
   * When there is no more page to read, total_pages_.
   */
  uint32_t              buffer_pos_;
  /** number of pages so far read in the buffer_. upto total_pages_. */
  uint32_t              buffer_count_;

  /**
   * Cursor position for page in the buffer.
   * @invariant cursor_buffer_ <= buffer_count_
   */
  uint32_t              cursor_buffer_;

  /**
   * Cursor position for bin in the current page.
   * @invariant cursor_bin_ <= cursor_bin_count_
   */
  uint16_t              cursor_bin_;
  /** Number of active bins in the current page. */
  uint16_t              cursor_bin_count_;

  /** The buffer to read contiguous pages in one shot */
  HashComposedBinsPage* buffer_;

  void init(
    cache::SnapshotFileSet* fileset,
    SnapshotPagePointer head_page_id,
    uint32_t total_pages,
    uint32_t buffer_size,
    HashComposedBinsPage* buffer);

  /**
   * If needed, expand the given read buffer to be used with the inputs.
   * We split the one read buffer for inputs, so each input might not receive
   * a large enough buffer to read HashComposedBinsPage.
   */
  static void assure_read_buffer_size(memory::AlignedMemory* read_buffer, uint32_t inputs);

  /**
   * Read pages to buffer_. This doesn't return error even when
   * buffer_pos_ + buffer_cursor_count_ ==total_pages_,
   * in which case this method does nothing. Use has_more() to check for that.
   * @pre cursor_bin_ == cursor_bin_count_, otherwise you are skipping some bin
   * @pre cursor_buffer_ == buffer_count_, otherwise you are skipping some page
   */
  ErrorCode next_pages();
  /** Moves on to next bin. */
  ErrorCode next_bin() ALWAYS_INLINE;
  /**
   * @returns the page the cursor is currently on
   * @pre has_more()
   */
  const HashComposedBinsPage& get_cur_page() const ALWAYS_INLINE {
    return buffer_[cursor_buffer_];
  }
  /**
   * @returns the bin the cursor is currently on
   * @pre has_more()
   */
  const ComposedBin& get_cur_bin() const ALWAYS_INLINE {
    ASSERT_ND(has_more());
    ASSERT_ND(cursor_bin_count_ == get_cur_page().bin_count_);
    ASSERT_ND(cursor_bin_ < cursor_bin_count_);
    return get_cur_page().bins_[cursor_bin_];
  }
  bool has_more() const { return buffer_pos_ < total_pages_; }
};

/**
 * @brief Packages all ComposedBinsBuffer to easily extract bins of current interest.
 * @ingroup HASH
 * @details
 * This is the highest-level of the object hierarchy defined in this file.
 */
struct ComposedBinsMergedStream final {
  typedef HashIntermediatePage* PagePtr;

  /** just for auto release */
  std::unique_ptr< ComposedBinsBuffer[] > inputs_memory_;

  ComposedBinsBuffer*   inputs_;
  /** total number of buffers in inputs_ */
  uint32_t              input_count_;
  snapshot::SnapshotId  snapshot_id_;
  uint8_t               levels_;
  uint8_t               padding_;

  /** The pages we are now composing. cur_path_[n] is the level-n intermediate page. */
  PagePtr               cur_path_[kHashMaxLevels];

  ErrorStack init(
    const HashRootInfoPage**  inputs,
    uint32_t                  input_count,
    PagePtr                   root_page,
    uint16_t                  root_child_index,
    memory::AlignedMemory*    read_buffer,
    cache::SnapshotFileSet*   fileset,
    snapshot::SnapshotWriter* writer,
    uint32_t*                 writer_buffer_pos,
    uint32_t*                 writer_higher_buffer_pos);

  /**
   * Consumes inputs for the cur_path_[0] page and install snapshot pointers there.
   * @param[in,out] installed_count Increments the number of installed pointers
   * @param[out] next_lowest_bin The smallest bin out of all remaining inputs.
   * kInvalidHashBin if all of them are depleted.
   */
  ErrorCode process_a_bin(uint32_t* installed_count, HashBin* next_lowest_bin);

  /**
   *
   */
  ErrorCode switch_path(HashBin lowest_bin, snapshot::SnapshotWriter* writer);
};

inline ErrorCode ComposedBinsBuffer::next_bin() {
  if (LIKELY(cursor_bin_ < cursor_bin_count_)) {
    ++cursor_bin_;
    return kErrorCodeOk;
  } else if (LIKELY(cursor_buffer_ < buffer_count_)) {
    ++cursor_buffer_;
    cursor_bin_ = 0;
    cursor_bin_count_ = buffer_[cursor_buffer_].bin_count_;
    return kErrorCodeOk;
  } else {
    return next_pages();
  }
}

static_assert(sizeof(HashRootInfoPage) == kPageSize, "incorrect sizeof(RootInfoPage)");
static_assert(sizeof(HashComposedBinsPage) == kPageSize, "incorrect sizeof(HashComposedBinsPage)");

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_COMPOSED_BINS_IMPL_HPP_
