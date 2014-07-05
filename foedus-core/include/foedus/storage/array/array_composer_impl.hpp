/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_COMPOSER_IMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_COMPOSER_IMPL_HPP_

#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/array/fwd.hpp"

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
class ArrayComposer final : public virtual Composer {
 public:
  ArrayComposer(
    Engine *engine,
    const ArrayPartitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    const snapshot::Snapshot& new_snapshot);
  ~ArrayComposer() {}

  ArrayComposer() = delete;
  explicit ArrayComposer(const ArrayPartitioner& other) = delete;
  ArrayComposer& operator=(const ArrayPartitioner& other) = delete;

  std::string to_string() const override;
  void describe(std::ostream* o) const override;

  ErrorStack compose(
    snapshot::SortedBuffer** log_streams,
    uint32_t log_streams_count,
    SnapshotPagePointer previous_root_page_pointer,
    const memory::AlignedMemorySlice& work_memory,
    Page* root_info_page) override;

  uint64_t get_required_work_memory_size(
    snapshot::SortedBuffer** log_streams,
    uint32_t log_streams_count) const override;

 private:
  Engine* const engine_;
  const ArrayPartitioner* const partitioner_;
  snapshot::SnapshotWriter* const snapshot_writer_;
  const snapshot::Snapshot& new_snapshot_;

  ErrorStack strawman_tournament(
    snapshot::SortedBuffer** log_streams,
    uint32_t log_streams_count,
    SnapshotPagePointer previous_root_page_pointer,
    const memory::AlignedMemorySlice& work_memory);
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_COMPOSER_IMPL_HPP_
