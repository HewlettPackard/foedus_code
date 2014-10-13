/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  ErrorStack replace_pointers(const Composer::ReplacePointersArguments& args);

  uint64_t get_required_work_memory_size_compose(
    snapshot::SortedBuffer** log_streams,
    uint32_t log_streams_count) const;

 private:
  Engine* const             engine_;
  const StorageId           storage_id_;
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

/** Represents one sorted input stream with its status. */
struct ArrayStreamStatus final {
  void init(snapshot::SortedBuffer* stream);
  ErrorCode next() ALWAYS_INLINE;
  void read_entry() ALWAYS_INLINE;
  const ArrayCommonUpdateLogType* get_entry() const ALWAYS_INLINE;

  snapshot::SortedBuffer* stream_;
  const char*     buffer_;
  uint64_t        buffer_size_;
  uint64_t        cur_absolute_pos_;
  uint64_t        cur_relative_pos_;
  uint64_t        end_absolute_pos_;
  ArrayOffset     cur_value_;
  xct::XctId      cur_xct_id_;
  uint32_t        cur_length_;
  bool            ended_;
};

/**
 * ArrayComposer's compose() implementation separated from the class itself.
 * It's a complicated method, so worth being its own class.
 * This defines all the variables maintained during one compose() call.
 */
class ArrayComposeContext {
 public:
  enum Constants {
    /**
     * Put this in flags if we are using volatile pointer to intermediate page's
     * to hold an index in intermediate buffer (relative to intermediate_base_).
     */
    kFlagIntermediatePointer = 0xDA,
  };
  ArrayComposeContext(
    Engine*                           engine,
    StorageId                         storage_id,
    snapshot::SnapshotWriter*         snapshot_writer,
    cache::SnapshotFileSet*           previous_snapshot_files,
    snapshot::SortedBuffer* const*    log_streams,
    uint32_t                          log_streams_count,
    const memory::AlignedMemorySlice& work_memory,
    Page*                             root_info_page);

  ErrorStack execute();

 private:
  ErrorStack init_more();
  /** sub routine of init_more to initialize cur_xxx with the first page. */
  ErrorStack init_context_cur_path();
  /** Same as above, but for the case where there was no previous snapshot */
  void init_context_empty_cur_path();

  ErrorStack finalize();
  ErrorCode advance() ALWAYS_INLINE;

  /** @return whether next key belongs to a different page */
  bool update_next_route() ALWAYS_INLINE;
  ErrorCode update_cur_path();
  const ArrayCommonUpdateLogType* get_next_entry() const ALWAYS_INLINE;

  /** @pre levels_ > level. */
  ArrayRange calculate_array_range(LookupRoute route, uint8_t level) const ALWAYS_INLINE;

  ErrorCode read_or_init_page(
    SnapshotPagePointer old_page_id,
    SnapshotPagePointer new_page_id,
    uint8_t level,
    LookupRoute route,
    ArrayPage* page) ALWAYS_INLINE;

  // these properties are initialized in constructor and never changed afterwards
  Engine* const                   engine_;
  const StorageId                 storage_id_;
  snapshot::SnapshotWriter* const snapshot_writer_;
  cache::SnapshotFileSet*  const  previous_snapshot_files_;
  /** The final output of the compose() call */
  ArrayRootInfoPage* const        root_info_page_;
  ArrayStreamStatus* const        inputs_;
  const uint32_t                  inputs_count_;

  // the followings are set in init_more()
  uint16_t                  payload_size_;
  uint8_t                   levels_;
  /** Calculates LookupRoute from offset. */
  LookupRouteFinder         route_finder_;
  SnapshotPagePointer       previous_root_page_pointer_;

  /**
  * The offset interval a single page represents in each level. index=level.
  * So, offset_intervals[0] is the number of records in a leaf page.
  */
  uint64_t                  offset_intervals_[kMaxLevels];

  uint32_t                  ended_inputs_count_;

  /**
  * path_[0] points to leaf we are now modifying, path_[1] points to its parent.
  * cur_path_[levels_-1] is always the root page, of course.
  */
  ArrayPage*                cur_path_[kMaxLevels];
  /** [0] means record ordinal in leaf, [1] in its parent page, [2]...*/
  LookupRoute               cur_route_;

  // this set of next_xxx indicates the min input to be applied next
  uint32_t                  next_input_;
  ArrayOffset               next_key_;
  xct::XctId                next_xct_id_;
  /** [0] means record ordinal in leaf, [1] in its parent page, [2]...*/
  LookupRoute               next_route_;
  ArrayOffset               next_page_starts_;
  ArrayOffset               next_page_ends_;

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
  ArrayPage*                page_base_;
  /** same as snapshot_writer_->get_intermediate_base()*/
  ArrayPage*                intermediate_base_;
};

static_assert(sizeof(ArrayRootInfoPage) == kPageSize, "incorrect sizeof(RootInfoPage)");
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_COMPOSER_IMPL_HPP_
