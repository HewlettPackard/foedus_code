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
  ErrorStack replace_pointers(const Composer::ReplacePointersArguments& args);

 private:
  Engine* const             engine_;
  const StorageId           storage_id_;
  const MasstreeStorage     storage_;

  /** Rigorously check the input parameters of construct_root() */
  ErrorStack check_buddies(const Composer::ConstructRootArguments& args) const;
};


/**
 * Represents one sorted input stream with its status.
 * @todo extract common code with ArrayStreamStatus and sequential StreamStatus.
 */
struct MasstreeStreamStatus final {
  void init(snapshot::SortedBuffer* stream);
  ErrorCode next() ALWAYS_INLINE;
  const MasstreeCommonLogType* get_entry() const ALWAYS_INLINE;

  snapshot::SortedBuffer* stream_;
  const char*     buffer_;
  uint64_t        buffer_size_;
  uint64_t        cur_absolute_pos_;
  uint64_t        cur_relative_pos_;
  uint64_t        end_absolute_pos_;
  uint16_t        cur_log_length_;
  bool            ended_;
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
  };

  /**
   * One level in the path.
   * Each level consists of one or more pages with contiguous key ranges.
   * Initially, head==tail. Whenever tail becomes full, it adds a new page which becomes the new
   * tail.
   */
  struct PathLevel {
    /** Offset in page_base_*/
    memory::PagePoolOffset head_;
    /** Offset in page_base_*/
    memory::PagePoolOffset tail_;
    /** B-tri layer of this level */
    uint8_t   layer_;
    /** Whether this level is a border page */
    bool      border_;
    /**
     * If level is based on an existing snapshot page, the next entry (pointer or
     * record) in the original page to copy. Remember, we copy the entries when it is same as or
     * less than the next key so that we always append. 255 means no more entry to copy.
     */
    uint8_t   next_original_;
    /** for intermediate page */
    uint8_t   next_original_mini_;
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
    void set_no_more_next_original() { next_original_ = 0xFFU; }
    bool contains_slice(KeySlice slice) const {
      ASSERT_ND(low_fence_ <= slice);  // as logs are sorted, this must not happen
      return high_fence_ == kSupremumSlice || slice < high_fence_;  // careful on supremum case
    }

    friend std::ostream& operator<<(std::ostream& o, const PathLevel& v);
  };

  struct FenceAndPointer {
    KeySlice low_fence_;
    SnapshotPagePointer pointer_;
  };

  MasstreeComposeContext(Engine* engine, StorageId id, const Composer::ComposeArguments& args);
  ErrorStack execute();

  static ErrorStack assure_work_memory_size(const Composer::ComposeArguments& args);

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
  uint16_t get_cur_prefix_length() const { return cur_path_layers_ * sizeof(KeySlice); }
  uint8_t                   get_last_level_index() const {
    ASSERT_ND(cur_path_levels_ > 0);
    return cur_path_levels_ - 1U;
  }
  PathLevel*                get_last_level() { return cur_path_ + get_last_level_index(); }
  const PathLevel*          get_last_level() const  { return cur_path_ + get_last_level_index(); }
  PathLevel*                get_second_last_level() {
    ASSERT_ND(cur_path_levels_ > 1U);
    return cur_path_ + (cur_path_levels_ - 2U);
  }

  /** When the main buffer of writer has no page, appends a dummy page for easier debugging. */
  void        write_dummy_page_zero();

  // init/uninit called only once or at most a few times
  ErrorStack  init_root();
  ErrorStack  init_inputs();
  ErrorStack  open_first_level(const char* key, uint16_t key_length);
  ErrorStack  open_next_level(
    const char* key,
    uint16_t key_length,
    MasstreePage* parent,
    SnapshotPagePointer* next_page_id);
  void        open_next_level_create_layer(MasstreeBorderPage* parent, uint8_t parent_index);
  ErrorStack  finalize();

  /**
   * @brief Writes out the main buffer in the writer to the file.
   * @post root_page_id_ is set to snapshot page ID of the root of first layer that is written out.
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

  ErrorStack  adjust_path(const char* key, uint16_t key_length);
  /**
   * Close the root of first layer. This is called from flush_buffer().
   * @pre cur_path_levels_ == 1
   * @post root_page_id_ is set to snapshot page ID of the root of first layer.
   */
  ErrorStack  close_first_level();

  ErrorStack  consume_original_all();
  ErrorStack  grow_layer_root(SnapshotPagePointer* root_pointer);

  // next methods called for each log entry. must be efficient! So these methods return ErrorCode.

  /** Find the log entry to apply next. so far this is a dumb sequential search. loser tree? */
  void        read_inputs() ALWAYS_INLINE;
  ErrorCode   advance() ALWAYS_INLINE;
  /** Returns if the given key is not contained in the current path */
  bool        does_need_to_adjust_path(const char* key, uint16_t key_length) const ALWAYS_INLINE;

  /**
   * Close last levels whose layer is deeper than max_layer
   * @pre cur_path_layers_ > max_layer
   * @post cur_path_layers_ == max_layer
   */
  ErrorStack  close_path_layer(uint16_t max_layer);
  /**
   * Close the last (deepest) level.
   * @pre cur_path_levels_ > 1 (this method must not be used to close the root of first layer.)
   */
  ErrorStack  close_last_level();

  ErrorCode   read_original_page(SnapshotPagePointer page_id, uint16_t path_level);

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

  Engine* const             engine_;
  const StorageId           id_;
  const MasstreeStorage     storage_;
  const Composer::ComposeArguments& args_;

  const uint16_t            numa_node_;
  const uint32_t            max_pages_;
  // const uint32_t            max_intermediates_;

  /**
   * Root of first layer, which is the joint point for partitioner and composer.
   * This is not on the writer buffer, but the root_info_page shared memory.
   */
  MasstreeIntermediatePage* const root_;

  /** same as snapshot_writer_->get_page_base() */
  Page* const       page_base_;
  /** same as work_memory_->get_block(). Index is level. */
  Page* const       original_base_;
  /** same as work_memory_->get_block() + sizeof(Page)*(kMaxLevels+1) */
  MasstreeStreamStatus* const inputs_;

  // const members up to here.

  /**
   * This value plus offset in page_base_ will be the final snapshot page ID when the pages are
   * written out. This value is changed for each buffer flush.
   */
  SnapshotPagePointer       page_id_base_;
  /**
   * 'Previous' snapshot's root page ID. This might be the ID of tentative snapshot page
   * written by the flush_buffer.
   */
  SnapshotPagePointer       root_page_id_;

  /**
   * How many pages we allocated in the main buffer of args_.snapshot_writer.
   * This is reset to zero for each buffer flush, then immediately incremented to 1 as we always
   * output a dummy page to avoid offset-0 (for easier debugging, not mandatory).
   */
  uint32_t                  allocated_pages_;
  /**
   * How many pages we allocated in the intermediate-page buffer of args_.snapshot_writer.
   * This is purely monotonially increasing beecause we keep intermediate pages until the end
   * of compose().
   */
  // uint32_t                  allocated_intermediates_;

  uint32_t                  ended_inputs_count_;

  uint32_t                  next_input_;
  /** Same as inputs_[next_input_].get_entry() */
  const MasstreeCommonLogType* next_entry_;

  /** Number of cur_route_ entries that are now active. Does not count root_ as a level. */
  uint8_t                   cur_path_levels_;
  /** Layer of the last level. */
  uint8_t                   cur_path_layers_;
  /** Page path to the currently opened page. [0] to [cur_path_levels_-1] are opened levels. */
  PathLevel                 cur_path_[kMaxLevels];
  /** Prefix slice of B-tree layers upto cur_path_layers_. Remember level != layer. */
  KeySlice                  cur_prefix_slices_[kMaxLayers];
  /** Prefix slice in the original big-endian format. */
  char                      cur_prefix_be_[kMaxLayers * sizeof(KeySlice)];
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_COMPOSER_IMPL_HPP_
