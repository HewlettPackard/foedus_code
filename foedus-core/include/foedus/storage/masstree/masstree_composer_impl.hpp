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

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Composer for an masstree storage.
 * @ingroup MASSTREE
 * @details
 * @note
 * This is a private implementation-details of \ref MASSTREE, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class MasstreeComposer final : public virtual Composer {
 public:
  /** Output of one compose() call, which are then combined in construct_root(). */
  struct RootInfoPage final {
    PageHeader          header_;          // +16 -> 16
    // TODO(Hideaki): properties
  };

  MasstreeComposer(
    Engine *engine,
    const MasstreePartitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    cache::SnapshotFileSet* previous_snapshot_files,
    const snapshot::Snapshot& new_snapshot);
  ~MasstreeComposer() {}

  MasstreeComposer() = delete;
  explicit MasstreeComposer(const MasstreePartitioner& other) = delete;
  MasstreeComposer& operator=(const MasstreePartitioner& other) = delete;

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
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_COMPOSER_IMPL_HPP_
