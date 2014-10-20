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
 * foster_twin[0] points to it. More details below (Original page pointer).
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
    kMaxLevels = 64,
    /** We assume B-tri depth is at most this (prefix is at most 8*this value)*/
    kMaxLayers = 16,
  };
  /**
   * Flags for volatile pages. These are mainly for sanity checks so far.
   */
  enum VolatilePointerType {
    /** The pointer is null */
    kNullPage = 0,
    /** The pointer is an offset in original_base_ */
    kOriginalPage = 1,
    /** The pointer is an offset in page_base_ */
    kNextPage = 2,
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
    /**
     * Level of this entry. 0 means the first entry.
     * This value is also the offset in original_base_ for the original page (if exists).
     */
    uint8_t   level_;
    /** Whether this entry is based on an existing snapshot page. */
    bool      has_original_;
    /** B-tri layer of this level */
    uint8_t   layer_;
    uint8_t   reserved_;
    /** number of pages in this level including head/tail, without original page (so, initial=1). */
    uint32_t  page_count_;
    /** same as low_fence of head */
    KeySlice  low_fence_;
    /** same as high_fence of tail */
    KeySlice  high_fence_;
  };

  MasstreeComposeContext(Engine* engine, StorageId id, const Composer::ComposeArguments& args);
  ErrorStack execute();

  static ErrorStack assure_work_memory_size(const Composer::ComposeArguments& args);

 private:
  snapshot::SnapshotWriter* get_writer()  const { return args_.snapshot_writer_; }
  cache::SnapshotFileSet*   get_files()   const { return args_.previous_snapshot_files_; }
  MasstreePage*             get_page(memory::PagePoolOffset offset) const ALWAYS_INLINE;
  MasstreePage*             get_original(memory::PagePoolOffset offset) const ALWAYS_INLINE;
  uint16_t get_cur_prefix_length() const { return cur_path_layers_ * sizeof(KeySlice); }

  /** When the main buffer of writer has no page, appends a dummy page for easier debugging. */
  void                      write_dummy_page_zero();

  ErrorStack                init_inputs();
  ErrorCode                 read_inputs() ALWAYS_INLINE;
  ErrorCode                 advance() ALWAYS_INLINE;

  ErrorStack                finalize();

  Engine* const             engine_;
  const StorageId           id_;
  const MasstreeStorage     storage_;
  const Composer::ComposeArguments& args_;

  const uint32_t            max_pages_;
  // const uint32_t            max_intermediates_;

  /** same as snapshot_writer_->get_page_base()*/
  MasstreePage* const       page_base_;
  /** same as snapshot_writer_->get_intermediate_base()*/
  // MasstreePage* const       intermediate_base_;
  /** same as work_memory_->get_block() */
  MasstreePage* const       original_base_;
  /** same as work_memory_->get_block() + sizeof(MasstreePage)*(kMaxLevels+1) */
  MasstreeStreamStatus* const inputs_;

  // const members up to here.

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

  /** Number of cur_route_ entries that are now active. */
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
