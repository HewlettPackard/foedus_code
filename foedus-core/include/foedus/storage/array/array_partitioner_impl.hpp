/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_PARTITIONER_IMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_PARTITIONER_IMPL_HPP_

#include <stdint.h>

#include <cstring>
#include <iosfwd>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"

namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Partitioner for an array storage.
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
class ArrayPartitioner final {
 public:
  explicit ArrayPartitioner(Partitioner* parent);

  ErrorStack design_partition();
  bool is_partitionable() const;
  void partition_batch(
    PartitionId                     local_partition,
    const snapshot::LogBuffer&      log_buffer,
    const snapshot::BufferPosition* log_positions,
    uint32_t                        logs_count,
    PartitionId*                    results) const;

  void sort_batch(
    const snapshot::LogBuffer&        log_buffer,
    const snapshot::BufferPosition*   log_positions,
    uint32_t                          logs_count,
    const memory::AlignedMemorySlice& sort_buffer,
    Epoch                             base_epoch,
    snapshot::BufferPosition*         output_buffer,
    uint32_t*                         written_count) const;

  uint64_t  get_required_sort_buffer_size(uint32_t log_count) const;

  ArrayOffset get_array_size() const;
  uint8_t   get_array_levels() const;
  const PartitionId* get_bucket_owners() const;

  friend std::ostream& operator<<(std::ostream& o, const ArrayPartitioner& v);

 private:
  Engine* const               engine_;
  const StorageId             id_;
  PartitionerMetadata* const  metadata_;
  ArrayPartitionerData*       data_;
};

struct ArrayPartitionerData final {
  // only for reinterpret_cast
  ArrayPartitionerData() = delete;
  ~ArrayPartitionerData() = delete;

  /** whether this array has only one page, so no interior page nor partitioning. */
  bool                  array_single_page_;
  uint8_t               array_levels_;

  /** Size of the entire array. */
  ArrayOffset           array_size_;

  /** bucket = offset / bucket_size_. */
  ArrayOffset           bucket_size_;

  /** partition of each bucket. */
  PartitionId           bucket_owners_[kInteriorFanout];
};


}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_PARTITIONER_IMPL_HPP_
