/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_partitioner_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace array {

ArrayPartitioner::ArrayPartitioner(Engine* engine, StorageId id) {
  ArrayStorage storage = engine->get_storage_manager().get_array(id);
  ASSERT_ND(storage.exists());

  array_id_ = id;
  array_levels_ = storage.get_levels();
  array_size_ = storage.get_array_size();
  bucket_size_ = array_size_ / kInteriorFanout;
  bucket_size_div_ = assorted::ConstDiv(bucket_size_);

  ArrayStorageControlBlock* array = storage.get_control_block();
  const memory::GlobalVolatilePageResolver& resolver
    = engine->get_memory_manager().get_global_volatile_page_resolver();
  ArrayPage* root_page = reinterpret_cast<ArrayPage*>(
    resolver.resolve_offset(array->root_page_pointer_.volatile_pointer_));
  if (array->levels_ == 1) {
    ASSERT_ND(root_page->is_leaf());
    array_single_page_ = true;
  } else {
    ASSERT_ND(!root_page->is_leaf());
    array_single_page_ = false;

    // how many direct children does this root page have?
    std::vector<uint64_t> pages = ArrayStoragePimpl::calculate_required_pages(
      array_size_, storage.get_payload_size());
    ASSERT_ND(pages.size() == array->levels_);
    ASSERT_ND(pages[pages.size() - 1] == 1);  // root page
    uint16_t direct_children = pages[pages.size() - 2];

    // do we have enough direct children? if not, some partition will not receive buckets.
    // Although it's not a critical error, let's log it as an error.
    uint16_t total_partitions = engine->get_options().thread_.group_count_;
    ASSERT_ND(total_partitions > 1);  // if not, why we are coming here. it's a waste.

    if (direct_children < total_partitions) {
      LOG(ERROR) << "Warning-like error: This array doesn't have enough direct children in root"
        " page to assign partitions. #partitions=" << total_partitions << ", #direct children="
        << direct_children << ". array=" << storage;
    }

    // two paths. first path simply sees volatile/snapshot pointer and determines owner.
    // second path addresses excessive assignments, off loading them to needy ones.
    std::vector<uint16_t> counts(total_partitions, 0);
    const uint16_t excessive_count = (direct_children / total_partitions) + 1;
    std::vector<uint16_t> excessive_children;
    for (uint16_t child = 0; child < direct_children; ++child) {
      const DualPagePointer &pointer = root_page->get_interior_record(child);
      PartitionId partition;
      if (pointer.volatile_pointer_.components.offset != 0) {
        partition = pointer.volatile_pointer_.components.numa_node;
      } else {
        // if no volatile page, see snapshot page owner.
        partition = extract_numa_node_from_snapshot_pointer(pointer.snapshot_pointer_);
        // this ignores the case where neither snapshot/volatile page is there.
        // however, as we create all pages at ArrayStorage::create(), this so far never happens.
      }
      ASSERT_ND(partition < total_partitions);
      if (counts[partition] >= excessive_count) {
        excessive_children.push_back(child);
      } else {
        ++counts[partition];
        bucket_owners_[child] = partition;
      }
    }

    // just add it to the one with least assignments.
    // a stupid loop, but this part won't be a bottleneck (only 250 elements).
    for (uint16_t child : excessive_children) {
      PartitionId most_needy = 0;
      for (PartitionId partition = 1; partition < total_partitions; ++partition) {
        if (counts[partition] < counts[most_needy]) {
          most_needy = partition;
        }
      }

      ++counts[most_needy];
      bucket_owners_[child] = most_needy;
    }
  }
}

void ArrayPartitioner::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<ArrayPartitioner>"
      << "<array_id_>" << array_id_ << "</array_id_>"
      << "<array_size_>" << array_size_ << "</array_size_>"
      << "<bucket_size_>" << bucket_size_ << "</bucket_size_>";
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    o << "<range bucket=\"" << i << "\" partition=\"" << bucket_owners_[i] << "\" />";
  }
  o << "</ArrayPartitioner>";
}

void ArrayPartitioner::partition_batch(
  PartitionId /*local_partition*/,
  const snapshot::LogBuffer&      log_buffer,
  const snapshot::BufferPosition* log_positions,
  uint32_t                        logs_count,
  PartitionId*                    results) const {
  ASSERT_ND(is_partitionable());
  for (uint32_t i = 0; i < logs_count; ++i) {
    const ArrayOverwriteLogType *log = reinterpret_cast<const ArrayOverwriteLogType*>(
      log_buffer.resolve(log_positions[i]));
    ASSERT_ND(log->header_.log_type_code_ == log::kLogCodeArrayOverwrite);
    ASSERT_ND(log->header_.storage_id_ == array_id_);
    ASSERT_ND(log->offset_ < array_size_);
    uint64_t bucket = bucket_size_div_.div64(log->offset_);
    ASSERT_ND(bucket < kInteriorFanout);
    results[i] = bucket_owners_[bucket];
  }
}

/**
  * Used in sort_batch().
  * \li 0-7 bytes: ArrayOffset, the most significant.
  * \li 8-9 bytes: compressed epoch (difference from base_epoch)
  * \li 10-11 bytes: in-epoch-ordinal
  * \li 12-15 bytes: BufferPosition (doesn't have to be sorted together, but for simplicity)
  * Be careful on endian! We use uint128_t to make it easier and faster.
  * @todo non-gcc support.
  */
struct SortEntry {
  inline void set(
    ArrayOffset               offset,
    uint16_t                  compressed_epoch,
    uint16_t                  in_epoch_ordinal,
    snapshot::BufferPosition  position) ALWAYS_INLINE {
    *reinterpret_cast<__uint128_t*>(this)
      = static_cast<__uint128_t>(offset) << 64
        | static_cast<__uint128_t>(compressed_epoch) << 48
        | static_cast<__uint128_t>(in_epoch_ordinal) << 32
        | static_cast<__uint128_t>(position);
  }
  inline ArrayOffset get_offset() const ALWAYS_INLINE {
    return static_cast<ArrayOffset>(
      *reinterpret_cast<const __uint128_t*>(this) >> 64);
  }
  inline snapshot::BufferPosition get_position() const ALWAYS_INLINE {
    return static_cast<snapshot::BufferPosition>(*reinterpret_cast<const __uint128_t*>(this));
  }
  char data_[16];
};

uint64_t ArrayPartitioner::get_required_sort_buffer_size(uint32_t log_count) const {
  // we so far sort them in one path.
  // to save memory, we could do multi-path merge-sort.
  // however, in reality each log has many bytes, so log_count is not that big.
  return sizeof(SortEntry) * log_count;
}

void ArrayPartitioner::sort_batch(
    const snapshot::LogBuffer&        log_buffer,
    const snapshot::BufferPosition*   log_positions,
    uint32_t                          log_positions_count,
    const memory::AlignedMemorySlice& sort_buffer,
    Epoch                             base_epoch,
    snapshot::BufferPosition*         output_buffer,
    uint32_t*                         written_count) const {
  if (log_positions_count == 0) {
    *written_count = 0;
    return;
  } else if (sort_buffer.get_size() < sizeof(SortEntry) * log_positions_count) {
    LOG(FATAL) << "Sort buffer is too small! log count=" << log_positions_count
      << ", buffer= " << sort_buffer
      << ", required=" << get_required_sort_buffer_size(log_positions_count);
  }

  debugging::StopWatch stop_watch_entire;

  ASSERT_ND(sizeof(SortEntry) == 16);
  const Epoch::EpochInteger base_epoch_int = base_epoch.value();
  SortEntry* entries = reinterpret_cast<SortEntry*>(sort_buffer.get_block());
  for (uint32_t i = 0; i < log_positions_count; ++i) {
    const ArrayOverwriteLogType* log_entry = reinterpret_cast<const ArrayOverwriteLogType*>(
      log_buffer.resolve(log_positions[i]));
    ASSERT_ND(log_entry->header_.log_type_code_ == log::kLogCodeArrayOverwrite);
    uint16_t compressed_epoch;
    const Epoch::EpochInteger epoch = log_entry->header_.xct_id_.get_epoch_int();
    if (epoch >= base_epoch_int) {
      ASSERT_ND(epoch - base_epoch_int < (1 << 16));
      compressed_epoch = epoch - base_epoch_int;
    } else {
      // wrap around
      ASSERT_ND(epoch + Epoch::kEpochIntOverflow - base_epoch_int < (1 << 16));
      compressed_epoch = epoch + Epoch::kEpochIntOverflow - base_epoch_int;
    }
    entries[i].set(
      log_entry->offset_,
      compressed_epoch,
      log_entry->header_.xct_id_.get_ordinal(),
      log_positions[i]);
  }

  debugging::StopWatch stop_watch;
  // TODO(Hideaki) non-gcc support.
  // Actually, we need only 12-bytes sorting, so perhaps doing without __uint128_t is faster?
  std::sort(
    reinterpret_cast<__uint128_t*>(entries),
    reinterpret_cast<__uint128_t*>(entries + log_positions_count));
  stop_watch.stop();
  VLOG(0) << "Sorted " << log_positions_count
    << " log entries in " << stop_watch.elapsed_ms() << "ms";

  uint32_t result_count = 1;
  output_buffer[0] = entries[0].get_position();
  for (uint32_t i = 1; i < log_positions_count; ++i) {
    // compact the logs if the same offset appears in a row, and covers the same data region.
    // because we sorted it by offset and then ordinal, later logs can overwrite the earlier one.
    if (entries[i].get_offset() == entries[i - 1].get_offset()) {
      // is the data region same or superseded?
      const ArrayOverwriteLogType* prev = reinterpret_cast<const ArrayOverwriteLogType*>(
        log_buffer.resolve(entries[i - 1].get_position()));
      const ArrayOverwriteLogType* next = reinterpret_cast<const ArrayOverwriteLogType*>(
        log_buffer.resolve(entries[i].get_position()));
      uint16_t prev_begin = prev->payload_offset_;
      uint16_t prev_end = prev_begin + prev->payload_count_;
      uint16_t next_begin = next->payload_offset_;
      uint16_t next_end = next_begin + next->payload_count_;
      if (next_begin <= prev_begin && next_end >= prev_end) {
        --result_count;
      }

      // the logic checks data range against only the previous entry.
      // we might have a situation where 3 or more log entries have the same array offset
      // and the data regions are like following
      // Log 1: [4, 8) bytes, Log 2: [8, 12) bytes, Log 3: [4, 8) bytes
      // If we check further, Log 3 can eliminate Log 1. However, the check is expensive..
    }
    output_buffer[result_count] = entries[i].get_position();
    ++result_count;
  }

  stop_watch_entire.stop();
  VLOG(0) << "Array-" << array_id_ << " sort_batch() done in  " << stop_watch_entire.elapsed_ms()
      << "ms  for " << log_positions_count << " log entries, compacted them to"
        << result_count << " log entries";
  *written_count = result_count;
}
}  // namespace array
}  // namespace storage
}  // namespace foedus
