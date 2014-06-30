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
 * There are a few options to implement partitioning for an array with trade-offs between
 * simplicity/efficiency and accuracy/flexibility.
 *
 * @par Current policy
 * So far our choice prefers simplicity/efficiency.
 * We split the whole range of the array into kInteriorFanout buckets and assign the partition
 * based on who currently holds the page under the root page.
 * Designing this policy is extremely simple; we just take a look at the root page of this storage
 * and sees the volatile pointer's NUMA node.
 *
 * @par Balancing policy
 * We so far balance the partition assignments so that no partitition receives
 * more than average buckets where average is buckets/partitions.
 * The excessive bucket is given to needy ones that do not have enough buckets.
 *
 * @par Limitations of current policy
 * Of course this simple policy has some issue. One issue is that if the root page has
 * direct children fewer than the number of partitions, some partition does not receive any
 * bucket even if there are many more indirect children. That doesn't happen so often, though.
 * We outputs warnings if this happens.
 *
 * @par Alternative policy
 * Another choice we considered was a vector of ArrayRange in an arbitrary length
 * over which we do binary search. However, this is more expensive.
 * For a simple data structure like array, it might not pay off.
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
    const memory::AlignedMemorySlice& work_memory) override;

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
