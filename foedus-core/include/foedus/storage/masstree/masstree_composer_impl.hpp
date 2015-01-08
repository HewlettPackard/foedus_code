/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_COMPOSER_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_COMPOSER_IMPL_HPP_

#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Composer for an masstree storage.
 * @ingroup MASSTREE
 * @details
 * This is the most complex composer out of the four storage types.
 *
 * @par Differences from volatile pages
 * There are a few fundamental differences from volatile world.
 * \li Very high fill factor. Almost 100%.
 * \li No foster children.
 * \li All keys are completely sorted by keys in boundary pages, which speeds up cursor.
 *
 * @par Constructing pages that match volatile pages well
 * One of the differences from the simpler array storage is that page boundaries depend on the data.
 * If the snapshot pages have all different page boundaries from volatile pages, the only
 * snapshot pointer we can install after snapshotting is the root pointer. We can only drop
 * the entire volatile pages in that case.
 * In the current version, this \b is the case. We drop everything and we don't consider
 * where's the page bounary in the volatile world. This is one of major todos.
 * However, it should be just an implementation issue. "Peeking" the volatile world's page boundary
 * is not a difficult thing especially because we don't care 100% accuracy; if the page boundary
 * doesn't match occasionally, that's fine. We just use it to loosely guide the choice of page
 * boundaries in snapshot pages.
 *
 * @note
 * This is a private implementation-details of \ref MASSTREE, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class MasstreeComposer final {
 public:
  explicit MasstreeComposer(Composer *parent);
  std::string to_string() const;

  ErrorStack compose(const Composer::ComposeArguments& args);
  ErrorStack construct_root(const Composer::ConstructRootArguments& args);
  bool drop_volatiles(const Composer::DropVolatilesArguments& args);

 private:
  Engine* const             engine_;
  const StorageId           storage_id_;
  const MasstreeStorage     storage_;

  /** Rigorously check the input parameters of construct_root() */
  ErrorStack check_buddies(const Composer::ConstructRootArguments& args) const;

  MasstreePage* resolve_volatile(VolatilePagePointer pointer);
  bool drop_volatiles_recurse(
    const Composer::DropVolatilesArguments& args,
    DualPagePointer* pointer);
  bool drop_volatiles_intermediate(
    const Composer::DropVolatilesArguments& args,
    MasstreeIntermediatePage* page);
  bool drop_volatiles_border(
    const Composer::DropVolatilesArguments& args,
    MasstreeBorderPage* page);
  bool is_updated_pointer(
    const Composer::DropVolatilesArguments& args,
    SnapshotPagePointer pointer) const;
  bool is_to_keep_volatile(uint8_t layer, uint16_t btree_level) const;

  /** used only when "page" is guaranteed to be dropped. */
  void drop_foster_twins(const Composer::DropVolatilesArguments& args, MasstreePage* page);
};


/**
 * @brief MasstreeComposer's compose() implementation separated from the class itself.
 * @ingroup MASSTREE
 * @details
 * It's a complicated method, so worth being its own class.
 * This defines all the variables maintained during one compose() call.
 *
 * @par Algorithm Overview
 * As described above, the biggest advantage to separate composer from transaction execution is
 * that it simplifies the logic to construct a page.
 * The sorted log stream provides log entries in key order. This class follows the same order
 * when it constructs a pages. For every key, we do:
 *  \li Make sure this object maintains pages in the path from root to the page that contains
 * the key.
 *  \li Each maintained page is initialized by previous snapshot page, empty if not exists.
 *  \li The way we initialize with snapshot page is a bit special. We keep the image of
 * the snapshot page as \e original page and append them to the maintained page up to the current
 * key. The original page is stored in work memory (because we will not write them out), and
 * PathLevel points to it. More details below (Original page pointer).
 *  \li Assuming the original page trick, each maintained page always appends a new record
 * to the end or update the record in the end, thus it's trivial to achieve 100% fill factor without
 * any record movement.
 *  \li Each maintained page might have a next-page pointer in foster_twin[1], which is
 * filled when the page becomes full (note: because keys are ordered, we never have to do the
 * split with records. it just puts a next-page pointer).
 *
 * @par Page path and open/close
 * We maintain one path of pages (route) from the root to a border page that contains the current
 * key. Each level consists of exactly one page with optional next page chain and original page.
 * As far as we keep appending a key in the page of last level, we keep the same path forever
 * upto the end of logs. The following cases trigger a change in the path.
 *  \li Creation of next layer. We just deepen the path for one level. This is the easiest change.
 *  \li Nexy key is beyond high-fence of the page of last level. We then \e close the last level,
 * insert to the parent, repeat until the ancestor that logically contains the key (potentially
 * root of the first layer), then \e open the path for next key.
 *  \li Full page buffer. When we can't add any more page in writer's buffer, we flush out the last
 * level page chain upto the tail page (the tail page is kept because it's currently modified).
 * This keeps the current path except that last level's head/low-fence are moved forward.
 *
 * @par Original page pointer
 * The \e active page in each level has a non-null pointer with kOriginalPage flag in foster_twin[0]
 * so that we can append remaining records/links when next key is same or above the keys.
 * The original page image is created when the page in the path is opened, and kept until it's
 * closed. Thus, there are at most #-of-levels original pages, which should be pretty small.
 */
class MasstreeComposeContext {
 public:
  enum Constants {
    /** We assume the path wouldn't be this deep. */
    kMaxLevels = 32,
    /** We assume B-trie depth is at most this (prefix is at most 8*this value)*/
    kMaxLayers = 16,
    /** Maximum number of logs execute_xxx_group methods process in one shot */
    kMaxLogGroupSize = 1 << 14,
    /**
     * Size of the tmp_boundary_array_.
     * Most likely we don't need this much, but this memory consumtion is negligible anyways.
     */
    kTmpBoundaryArraySize = 1 << 11,
  };

  /**
   * One level in the path.
   * Each level consists of one or more pages with contiguous key ranges.
   * Initially, head==tail. Whenever tail becomes full, it adds a new page which becomes the new
   * tail.
   * @note We move records in the original page when they are \e either
   *  \li Smaller, equal to, or containing (already next-layer pointer) the current key
   *  \li Will create a next layer with the current key
   * The second condition means that, even if the original record is larger than the current
   * key, we move it and trigger next layer creation. The reason behind this is that we don't
   * want to cause next layer creation when the deeper levels are already closed.
   * However, this approach instead violates "always append" policy. To work it around,
   * if the record was copied because of the second condition, we move the record
   * to a dummy original page in next layer, rather than the initial record. By doing so,
   * we still keep the always-append policy as well as guarantee that deeper levels never
   * receive records from upper levels.
   */
  struct PathLevel {
    /** Offset in page_base_*/
    memory::PagePoolOffset head_;
    /** Offset in page_base_*/
    memory::PagePoolOffset tail_;
    /** B-tri layer of this level */
    uint8_t   layer_;
    /**
     * If level is based on an existing snapshot page, the next entry (pointer or
     * record) in the original page to copy. Remember, we copy the entries when it is same as or
     * less than the next key so that we always append. 255 means no more entry to copy.
     */
    uint8_t   next_original_;
    /** for intermediate page */
    uint8_t   next_original_mini_;
    /** for border page. remaining key_length. 255 for next layer. */
    uint8_t   next_original_remaining_;
    /** number of pages in this level including head/tail, without original page (so, initial=1). */
    uint32_t  page_count_;
    /** same as low_fence of head */
    KeySlice  low_fence_;
    /** same as high_fence of tail */
    KeySlice  high_fence_;
    /**
     * Slice of next entry. Depending on the key length, this might be not enough to determine
     * the current key is smaller than or larger than that, in that case we have to go check
     * the entry each time (but hopefully that's rare).
     */
    KeySlice  next_original_slice_;

    bool has_next_original() const { return next_original_ != 0xFFU; }
    void set_no_more_next_original() {
      next_original_ = 0xFFU;
      next_original_mini_ = 0xFFU;
      next_original_remaining_ = 0xFFU;
      next_original_slice_ = kSupremumSlice;
    }
    bool contains_slice(KeySlice slice) const {
      ASSERT_ND(low_fence_ <= slice);  // as logs are sorted, this must not happen
      return high_fence_ == kSupremumSlice || slice < high_fence_;  // careful on supremum case
    }
    bool contains_key(const char* key, uint16_t key_length) const {
      ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));
      KeySlice slice = normalize_be_bytes_full_aligned(key + layer_ * kSliceLen);
      return contains_slice(slice);
    }
    bool needs_to_consume_original(KeySlice slice, uint16_t key_length) const {
      return has_next_original()
        && (
          next_original_slice_ < slice
          || (next_original_slice_ == slice
              && next_original_remaining_ > kSliceLen
              && key_length > (layer_ + 1U) * kSliceLen));
    }

    friend std::ostream& operator<<(std::ostream& o, const PathLevel& v);
  };

  struct FenceAndPointer {
    KeySlice low_fence_;
    SnapshotPagePointer pointer_;
  };

  /** Represents a minimal information to install a new snapshot page pointer */
  struct PageBoundaryInfo {
    uint8_t reserved_;
    /** set to true when this page is closed/reopened so that only one entry matches exactly */
    uint8_t removed_;
    /** B-trie layer of the new page. layer_+2 slices are stored. */
    uint8_t layer_;
    /** high 8-bits of SnapshotLocalPageId of the new page. */
    uint8_t local_snapshot_pointer_high_;
    /** low 32-bits of SnapshotLocalPageId of the new page. */
    uint32_t local_snapshot_pointer_low_;

    /**
     * actually of layer_+2. which is why sizeof does not work.
     * slices_[0] to slices_[layer_-1] store prefix slices.
     * slices_[layer_] is the low_fence, slices_[layer_+1] is the high fence of the new page.
     */
    KeySlice slices_[2];

    /** This object must be reinterpreted. Also, sizeof does not work. */
    PageBoundaryInfo() = delete;
    ~PageBoundaryInfo() = delete;

    uint32_t dynamic_sizeof() const ALWAYS_INLINE { return (layer_ + 2U) * sizeof(KeySlice) + 8U; }
    static uint32_t calculate_hash(
      uint8_t layer,
      const KeySlice* prefixes,
      KeySlice low_fence,
      KeySlice high_fence) ALWAYS_INLINE {
      // good old multiply-hash
      uint64_t hash = 0;
      const uint64_t kMult = 0x7ada20dc734afb6fULL;
      for (uint8_t i = 0; i < layer; ++i) {
        hash = hash * kMult + prefixes[i];
      }
      hash = hash * kMult + low_fence;
      hash = hash * kMult + high_fence;
      uint32_t compressed = static_cast<uint32_t>(hash >> 32) ^ static_cast<uint32_t>(hash);
      return compressed;
    }
    SnapshotLocalPageId get_local_page_id() const ALWAYS_INLINE {
      return static_cast<SnapshotLocalPageId>(local_snapshot_pointer_high_) << 32
        | static_cast<SnapshotLocalPageId>(local_snapshot_pointer_low_);
    }

    /**
     * returns whether the entry exactly matches with the page boundaries we look for.
     * There must be only one entry that exactly matches. To guarantee this, we have to remove
     * an old entry when we close/re-open the same page during this execution.
     * It should happen rarely (when flush_buffer() is called).
     * @see removed_
     */
    bool exact_match(
      uint8_t layer,
      const KeySlice* prefixes,
      KeySlice low,
      KeySlice high) const ALWAYS_INLINE {
      if (layer != layer_ || removed_) {
        return false;
      }
      for (uint8_t i = 0; i < layer_; ++i) {
        if (prefixes[i] != slices_[i]) {
          return false;
        }
      }
      return low == slices_[layer_] && high == slices_[layer_ + 1];
    }
  };

  /** Points to PageBoundaryInfo with a sorting information */
  struct PageBoundarySort {
    /**
     * Hash value of the entry. This is the sort key.
     * As we can install snapshot pointers only to pages whose fences exactly match, hashing works.
     */
    uint32_t                  hash_;
    /** Points to PageBoundaryInfo in page_boundary_info_ */
    snapshot::BufferPosition  info_pos_;
    /** used by std::sort */
    bool operator<(const PageBoundarySort& rhs) const ALWAYS_INLINE {
      return hash_ < rhs.hash_;
    }
    /** used by std::lower_bound */
    static bool static_less_than(
      const PageBoundarySort& lhs,
      const PageBoundarySort& rhs) ALWAYS_INLINE {
      return lhs.hash_ < rhs.hash_;
    }
  };

  MasstreeComposeContext(
    Engine* engine,
    snapshot::MergeSort* merge_sort,
    const Composer::ComposeArguments& args);
  ErrorStack execute();

 private:
  snapshot::SnapshotWriter* get_writer()  const { return args_.snapshot_writer_; }
  cache::SnapshotFileSet*   get_files()   const { return args_.previous_snapshot_files_; }
  memory::PagePoolOffset    allocate_page() {
    ASSERT_ND(allocated_pages_ < max_pages_);
    memory::PagePoolOffset new_offset = allocated_pages_;
    ++allocated_pages_;
    return new_offset;
  }

  MasstreePage*             get_page(memory::PagePoolOffset offset) const ALWAYS_INLINE;
  MasstreePage*             get_original(memory::PagePoolOffset offset) const ALWAYS_INLINE;
  MasstreeIntermediatePage* as_intermdiate(MasstreePage* page) const ALWAYS_INLINE;
  MasstreeBorderPage*       as_border(MasstreePage* page) const ALWAYS_INLINE;
  uint16_t get_cur_prefix_length() const { return get_last_layer() * sizeof(KeySlice); }
  uint8_t                   get_last_level_index() const {
    ASSERT_ND(cur_path_levels_ > 0);
    return cur_path_levels_ - 1U;
  }
  PathLevel*                get_last_level() { return cur_path_ + get_last_level_index(); }
  const PathLevel*          get_last_level() const  { return cur_path_ + get_last_level_index(); }
  uint8_t                   get_last_layer() const  {
    if (cur_path_levels_ == 0) {
      return 0;
    } else {
      return get_last_level()->layer_;
    }
  }
  void                      store_cur_prefix(uint8_t layer, KeySlice prefix_slice);

  PathLevel*                get_second_last_level() {
    ASSERT_ND(cur_path_levels_ > 1U);
    return cur_path_ + (cur_path_levels_ - 2U);
  }

  /**
   * execute() invokes this to process just one log that does not fit any of the following groups.
   * unlike other group-based method, this method must be much faster.
   * it's possible that we can't find a group, in which case this method is invoked for each log.
   */
  ErrorStack  execute_a_log(uint32_t cur);
  /**
   * execute() invokes this to process a number of contiguous logs that have the same key.
   * Optimization benefits:
   *  \li amortize the cost of adjust_path and in-page key comparison
   *  \li nullify all logs before delete-log.
   *
   * This often eliminates a large fraction of logs in a high-frequency insert-delete type of table.
   */
  ErrorStack  execute_same_key_group(uint32_t from, uint32_t to);
  /**
   * execute() invokes this to process a number of contiguous insert-logs.
   * Optimization benefits:
   *  \li amortize the cost of adjust_path
   *  \li amortize the cost of peeking to determine page boundaries
   *
   * This is one of the most important optimizations. Lots of inserts as table-load or creating a
   * new sub-tree is quite common.
   * We have to also choose a right page boundary in this case, so we use peeking to get hints.
   */
  ErrorStack  execute_insert_group(uint32_t from, uint32_t to);
  /**
   * Sub routine of execute_insert_group().
   * @return the number of logs from cur that can be processed without considering page-shift,
   * next-layer, or original-records. 0 means we have to process the log as usual.
   */
  uint32_t    execute_insert_group_get_cur_run(
    uint32_t cur,
    uint32_t to,
    KeySlice* min_slice,
    KeySlice* max_slice);
  /** Sub routine of execute_insert_group(). The tight loop to actually append records. */
  ErrorCode   execute_insert_group_append_loop(uint32_t from, uint32_t to, uint32_t hint_count);
  /**
   * execute() invokes this to process a number of contiguous delete-logs.
   * Optimization benefits:
   *  \li amortize the cost of adjust_path (per page. still page-shift is required)
   *
   * This is not a so common case. Optimization not implemented yet.
   */
  ErrorStack  execute_delete_group(uint32_t from, uint32_t to);
  /**
   * execute() invokes this to process a number of contiguous overwrite-logs.
   * Optimization benefits:
   *  \li amortize the cost of adjust_path (per page. still page-shift is required)
   *
   * This one is so-so common. Anyway this one is simple as there is no page split/merge.
   */
  ErrorStack  execute_overwrite_group(uint32_t from, uint32_t to);

  /** When the main buffer of writer has no page, appends a dummy page for easier debugging. */
  void        write_dummy_page_zero();

  // init/uninit called only once or at most a few times
  ErrorStack  init_root();
  ErrorStack  open_first_level(const char* key, uint16_t key_length);
  ErrorStack  open_next_level(
    const char* key,
    uint16_t key_length,
    MasstreePage* parent,
    KeySlice prefix_slice,
    SnapshotPagePointer* next_page_id);
  void        open_next_level_create_layer(
    const char* key,
    uint16_t key_length,
    MasstreeBorderPage* parent,
    KeySlice prefix_slice,
    uint8_t parent_index);

  /**
   * @brief Writes out the main buffer in the writer to the file.
   * @details
   * This method is occasionally called to flush out buffered pages and make room for further
   * processing. Unlike the simpler array, we have to also write out higher-level pages
   * because we don't know which pages will end up being "higher-level" in masstree.
   * Thus, this method consists of a few steps.
   *  \li Close all path levels to finalize as many pages as possible. This creates a few new pages
   * in the buffer, so this method must be called when there are still some rooms in the buffer
   * (eg 80% full).
   *  \li Write out all pages. Remember the snapshot-page ID of the (tentative) first-root page.
   *  \li Re-read the first-root page just like the initialization steps.
   *  \li Resume log processing. When needed, the written-out pages are re-read as original pages.
   *
   * Assuming buffer size is reasonably large, this method is only occasionally called.
   * Writing-out and re-reading the tentative pages in pathway shouldn't be a big issue.
   */
  ErrorStack  flush_buffer();
  inline ErrorStack flush_if_nearly_full() {
    uint64_t threshold = max_pages_ * 8ULL / 10ULL;
    if (UNLIKELY(allocated_pages_ >= threshold || allocated_pages_ + 256U >= max_pages_)) {
      CHECK_ERROR(flush_buffer());
    }
    return kRetOk;
  }

  ErrorStack  adjust_path(const char* key, uint16_t key_length);

  ErrorStack  consume_original_upto_border(KeySlice slice, uint16_t key_length, PathLevel* level);
  ErrorStack  consume_original_upto_intermediate(KeySlice slice, PathLevel* level);
  ErrorStack  consume_original_all();
  /**
   * Invoked from close_xxx_level when it results in a new level on top of the closed level.
   * This method replaces the closed level with a newly created level.
   */
  ErrorStack  close_level_grow_subtree(
    SnapshotPagePointer* root_pointer,
    KeySlice subtree_low,
    KeySlice subtree_high);
  /**
   * Used to close a level that is not a root of B-tree (either first layer or not).
   * Pushes up all pages to parent level, which is guaranteed to be an intermediate page because
   * this level is non-root.
   */
  ErrorStack  pushup_non_root();

  // next methods called for each log entry. must be efficient! So these methods return ErrorCode.

  /** Returns if the given key is not contained in the current path */
  bool        does_need_to_adjust_path(const char* key, uint16_t key_length) const ALWAYS_INLINE;

  /**
   * Close last levels whose layer is deeper than max_layer
   * @pre get_last_layer() > max_layer
   * @post get_last_layer() == max_layer
   */
  ErrorStack  close_path_layer(uint16_t max_layer);
  /**
   * Close the last (deepest) level.
   * @pre cur_path_levels_ > 1 (this method must not be used to close the root of first layer.)
   */
  ErrorStack  close_last_level();
  /**
   * Close the root of first layer. This is called from flush_buffer().
   * @pre cur_path_levels_ == 1
   * @post cur_path_levels_ == 0
   */
  ErrorStack  close_first_level();
  ErrorStack  close_all_levels();

  void        append_border(
    KeySlice slice,
    xct::XctId xct_id,
    uint16_t remaining_length,
    const char* suffix,
    uint16_t payload_count,
    const char* payload,
    PathLevel* level);
  void        append_border_next_layer(
    KeySlice slice,
    xct::XctId xct_id,
    SnapshotPagePointer pointer,
    PathLevel* level);
  void        append_border_newpage(KeySlice slice, PathLevel* level);
  void        append_intermediate(
    KeySlice low_fence,
    SnapshotPagePointer pointer,
    PathLevel* level);
  void        append_intermediate_newpage_and_pointer(
    KeySlice low_fence,
    SnapshotPagePointer pointer,
    PathLevel* level);


  //// page_boundary_info/install_pointers related
  void close_level_register_page_boundaries();
  void remove_old_page_boundary_info(SnapshotPagePointer page_id, MasstreePage* page);
  PageBoundaryInfo* get_page_boundary_info(snapshot::BufferPosition pos) ALWAYS_INLINE {
    ASSERT_ND(pos <= page_boundary_info_cur_pos_);
    return reinterpret_cast<PageBoundaryInfo*>(page_boundary_info_ + pos * 8ULL);
  }
  const PageBoundaryInfo* get_page_boundary_info(snapshot::BufferPosition pos) const ALWAYS_INLINE {
    ASSERT_ND(pos <= page_boundary_info_cur_pos_);
    return reinterpret_cast<PageBoundaryInfo*>(page_boundary_info_ + pos * 8ULL);
  }
  /** checks that the entry does not exist yet. wiped out in release mode */
  void assert_page_boundary_not_exists(
    uint8_t layer,
    const KeySlice* prefixes,
    KeySlice low,
    KeySlice high) const ALWAYS_INLINE;
  void sort_page_boundary_info();
  SnapshotPagePointer lookup_page_boundary_info(
    uint8_t layer,
    const KeySlice* prefixes,
    KeySlice low,
    KeySlice high) const ALWAYS_INLINE;
  ErrorStack install_snapshot_pointers(uint64_t* installed_count) const;
  ErrorCode install_snapshot_pointers_recurse(
    const memory::GlobalVolatilePageResolver& resolver,
    uint8_t layer,
    KeySlice* prefixes,
    MasstreePage* volatile_page,
    uint64_t* installed_count) const ALWAYS_INLINE;
  ErrorCode install_snapshot_pointers_recurse_intermediate(
    const memory::GlobalVolatilePageResolver& resolver,
    uint8_t layer,
    KeySlice* prefixes,
    MasstreeIntermediatePage* volatile_page,
    uint64_t* installed_count) const;
  ErrorCode install_snapshot_pointers_recurse_border(
    const memory::GlobalVolatilePageResolver& resolver,
    uint8_t layer,
    KeySlice* prefixes,
    MasstreeBorderPage* volatile_page,
    uint64_t* installed_count) const;

  Engine* const             engine_;
  snapshot::MergeSort* const  merge_sort_;
  const StorageId           id_;
  MasstreeStorage           storage_;
  const Composer::ComposeArguments& args_;

  const snapshot::SnapshotId snapshot_id_;
  const uint16_t            numa_node_;
  const uint32_t            max_pages_;
  /** max size of page_boundary_info_. bytes/8 */
  const snapshot::BufferPosition  page_boundary_info_capacity_;
  /** maximum number of page_boundary_info_elements_ */
  const uint32_t            max_page_boundary_info_;

  /**
   * Root of first layer, which is the joint point for partitioner and composer.
   * This is not on the writer buffer, but the root_info_page shared memory.
   */
  MasstreeIntermediatePage* const root_;

  /** same as snapshot_writer_->get_page_base() */
  Page* const       page_base_;
  /** backed by work memory in merge_sort_. Index is level. */
  Page* const       original_base_;

  /**
   * backed by the snapshot_writer's intermediate page memory.
   * In this composer, we don't use intermediate page memory. Instead, we use it to store
   * only minimal information we need later (when we install snapshot page pointers).
   * Each element is actually of type PageBoundaryInfo, but we must use byte positions to
   * obtain each element because PageBoundaryInfo does not allow sizeof.
   */
  char* const       page_boundary_info_;
  /** Sorting entries for page_boundary_info_. */
  PageBoundarySort* const page_boundary_sort_;

  // const members up to here.

  /**
   * This value plus offset in page_base_ will be the final snapshot page ID when the pages are
   * written out. This value is changed for each buffer flush.
   */
  SnapshotPagePointer       page_id_base_;

  /** How much we filled in page_boundary_info_. bytes/8. */
  snapshot::BufferPosition  page_boundary_info_cur_pos_;
  /** number of elements in page_boundary_info_ */
  uint32_t                  page_boundary_info_elements_;

  /**
   * How many pages we allocated in the main buffer of args_.snapshot_writer.
   * This is reset to zero for each buffer flush, then immediately incremented to 1 as we always
   * output a dummy page to avoid offset-0 (for easier debugging, not mandatory).
   */
  uint32_t                  allocated_pages_;

  /**
   * The index of the pointer we followed from the root_ to first level.
   * When cur_path_levels_ == 0, no meaning.
   */
  uint8_t                   root_index_;
  /**
   * The minipage index of the pointer we followed from the root_ to first level.
   * When cur_path_levels_ == 0, no meaning.
   */
  uint8_t                   root_index_mini_;

  /** Number of cur_route_ entries that are now active. Does not count root_ as a level. */
  uint8_t                   cur_path_levels_;
  /** Page path to the currently opened page. [0] to [cur_path_levels_-1] are opened levels. */
  PathLevel                 cur_path_[kMaxLevels];
  /** Prefix slice in the original big-endian format, upto get_last_layer() * kSliceLen. */
  char                      cur_prefix_be_[kMaxLayers * kSliceLen];
  /** Prefix slice in native order */
  KeySlice                  cur_prefix_slices_[kMaxLayers];
  /** only used in execute_insert_group() */
  KeySlice                  tmp_boundary_array_[kTmpBoundaryArraySize];
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_COMPOSER_IMPL_HPP_
