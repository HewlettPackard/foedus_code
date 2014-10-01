/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_COMPOSER_IMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_COMPOSER_IMPL_HPP_

#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/**
 * @brief Composer for an sequential storage.
 * @ingroup SEQUENTIAL
 * @details
 * Like partitioner, this does a quite simple stuff.
 * We don't need to do any merge-sort as there is no order.
 * We just sequentially add them all.
 *
 * @par Page allcation in compose()
 * This composer sequentially writes out data pages until the main buffer in snapshot_writer_
 * becomes full. Whenever it does, it writes out all the pages and treat the first page
 * as one head page. So, this compose() can output more than one head pages.
 * By doing this, we don't have to worry about any of the intermediate pages and pointer
 * installations. Sooooo simple.
 * The limit is of course 500 pointers (4kb), but surely it will fit.
 * If it doesn't, we must consider allowing variable-sized root info page.
 *
 * @note
 * This is a private implementation-details of \ref SEQUENTIAL, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class SequentialComposer final : public Composer {
 public:
  /** Output of one compose() call, which are then combined in construct_root(). */
  struct RootInfoPage final {
    PageHeader          header_;          // +16 -> 16
    /** Number of pointers stored in this page. */
    uint32_t            pointer_count_;   // +4 -> 20
    uint32_t            dummy_;           // +4 -> 24
    /** Pointers to head pages. */
    SnapshotPagePointer pointers_[(kPageSize - 24) / 8];  // -> 4096
  };

  SequentialComposer(
    Engine *engine,
    StorageId storage_id,
    snapshot::SnapshotWriter* snapshot_writer,
    cache::SnapshotFileSet* previous_snapshot_files,
    const snapshot::Snapshot& new_snapshot);
  ~SequentialComposer() {}

  SequentialComposer() = delete;
  explicit SequentialComposer(const SequentialPartitioner& other) = delete;
  SequentialComposer& operator=(const SequentialPartitioner& other) = delete;

  std::string to_string() const override;
  void describe(std::ostream* o) const override;

  ErrorStack compose(
    snapshot::SortedBuffer* const* log_streams,
    uint32_t log_streams_count,
    const memory::AlignedMemorySlice& work_memory,
    Page* root_info_page) override;

  ErrorStack construct_root(
    const Page* const*  root_info_pages,
    uint32_t            root_info_pages_count,
    const memory::AlignedMemorySlice& work_memory,
    SnapshotPagePointer* new_root_page_pointer) override;

  uint64_t get_required_work_memory_size(
    snapshot::SortedBuffer** /*log_streams*/,
    uint32_t /*log_streams_count*/) const override {
    return 0;
  }

 private:
  SequentialPage*     compose_new_head(RootInfoPage* root_info_page);
};

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_COMPOSER_IMPL_HPP_
