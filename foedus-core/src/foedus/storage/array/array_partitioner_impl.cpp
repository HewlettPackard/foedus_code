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
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace array {

ArrayPartitioner::ArrayPartitioner(Partitioner* parent)
  : engine_(parent->get_engine()),
    id_(parent->get_storage_id()),
    metadata_(PartitionerMetadata::get_metadata(engine_, id_)) {
  ASSERT_ND(metadata_->mutex_.is_initialized());
  if (metadata_->valid_) {
    data_ = reinterpret_cast<ArrayPartitionerData*>(metadata_->locate_data(engine_));
  } else {
    data_ = nullptr;
  }
}

ArrayOffset ArrayPartitioner::get_array_size() const {
  ASSERT_ND(data_);
  return data_->array_size_;
}

uint8_t ArrayPartitioner::get_array_levels() const {
  ASSERT_ND(data_);
  return data_->array_levels_;
}
const PartitionId* ArrayPartitioner::get_bucket_owners() const {
  ASSERT_ND(data_);
  return data_->bucket_owners_;
}
bool ArrayPartitioner::is_partitionable() const {
  ASSERT_ND(data_);
  return data_->partitionable_;
}

ErrorStack ArrayPartitioner::design_partition(
  const Partitioner::DesignPartitionArguments& /*args*/) {
  ASSERT_ND(metadata_->mutex_.is_initialized());
  ASSERT_ND(data_ == nullptr);
  ArrayStorage storage(engine_, id_);
  ASSERT_ND(storage.exists());
  ArrayStorageControlBlock* control_block = storage.get_control_block();

  soc::SharedMutexScope mutex_scope(&metadata_->mutex_);
  ASSERT_ND(!metadata_->valid_);
  WRAP_ERROR_CODE(metadata_->allocate_data(engine_, &mutex_scope, sizeof(ArrayPartitionerData)));
  data_ = reinterpret_cast<ArrayPartitionerData*>(metadata_->locate_data(engine_));

  data_->array_levels_ = storage.get_levels();
  data_->array_size_ = storage.get_array_size();

  if (storage.get_levels() == 1U || engine_->get_soc_count() == 1U) {
    // No partitioning needed.
    data_->bucket_owners_[0] = 0;
    data_->partitionable_ = false;
    data_->bucket_size_ = data_->array_size_;
    metadata_->valid_ = true;
    return kRetOk;
  }

  data_->partitionable_ = true;
  ASSERT_ND(storage.get_levels() >= 2U);

  // bucket size is interval that corresponds to a direct child of root.
  // eg) levels==2 : leaf, levels==3: leaf*kInteriorFanout, ...
  data_->bucket_size_ = control_block->route_finder_.get_records_in_leaf();
  for (uint32_t level = 1; level < storage.get_levels() - 1U; ++level) {
    data_->bucket_size_ *= kInteriorFanout;
  }

  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  // root page is guaranteed to have volatile version.
  ArrayPage* root_page = reinterpret_cast<ArrayPage*>(
    resolver.resolve_offset(control_block->root_page_pointer_.volatile_pointer_));
  ASSERT_ND(!root_page->is_leaf());

  // how many direct children does this root page have?
  uint16_t direct_children = storage.get_array_size() / data_->bucket_size_ + 1U;
  if (storage.get_array_size() % data_->bucket_size_ != 0) {
    ++direct_children;
  }

  // do we have enough direct children? if not, some partition will not receive buckets.
  // Although it's not a critical error, let's log it as an error.
  uint16_t total_partitions = engine_->get_options().thread_.group_count_;
  ASSERT_ND(total_partitions > 1U);  // if not, why we are coming here. it's a waste.

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
      data_->bucket_owners_[child] = partition;
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
    data_->bucket_owners_[child] = most_needy;
  }

  metadata_->valid_ = true;
  return kRetOk;
}

void ArrayPartitioner::partition_batch(const Partitioner::PartitionBatchArguments& args) const {
  if (!is_partitionable()) {
    std::memset(args.results_, 0, sizeof(PartitionId) * args.logs_count_);
    return;
  }

  ASSERT_ND(data_->bucket_size_ > 0);
  assorted::ConstDiv bucket_size_div(data_->bucket_size_);
  for (uint32_t i = 0; i < args.logs_count_; ++i) {
    const ArrayCommonUpdateLogType *log = reinterpret_cast<const ArrayCommonUpdateLogType*>(
      args.log_buffer_.resolve(args.log_positions_[i]));
    ASSERT_ND(log->header_.log_type_code_ == log::kLogCodeArrayOverwrite
        || log->header_.log_type_code_ == log::kLogCodeArrayIncrement);
    ASSERT_ND(log->header_.storage_id_ == id_);
    ASSERT_ND(log->offset_ < get_array_size());
    uint64_t bucket = bucket_size_div.div64(log->offset_);
    ASSERT_ND(bucket < kInteriorFanout);
    args.results_[i] = get_bucket_owners()[bucket];
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

void ArrayPartitioner::sort_batch(const Partitioner::SortBatchArguments& args) const {
  if (args.logs_count_ == 0) {
    *args.written_count_ = 0;
    return;
  }

  // we so far sort them in one path.
  // to save memory, we could do multi-path merge-sort.
  // however, in reality each log has many bytes, so log_count is not that big.
  args.work_memory_->assure_capacity(sizeof(SortEntry) * args.logs_count_);

  debugging::StopWatch stop_watch_entire;

  ASSERT_ND(sizeof(SortEntry) == 16);
  const Epoch::EpochInteger base_epoch_int = args.base_epoch_.value();
  SortEntry* entries = reinterpret_cast<SortEntry*>(args.work_memory_->get_block());
  for (uint32_t i = 0; i < args.logs_count_; ++i) {
    const ArrayCommonUpdateLogType* log_entry = reinterpret_cast<const ArrayCommonUpdateLogType*>(
      args.log_buffer_.resolve(args.log_positions_[i]));
    ASSERT_ND(log_entry->header_.log_type_code_ == log::kLogCodeArrayOverwrite
      || log_entry->header_.log_type_code_ == log::kLogCodeArrayIncrement);
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
      args.log_positions_[i]);
  }

  debugging::StopWatch stop_watch;
  // TODO(Hideaki) non-gcc support.
  // Actually, we need only 12-bytes sorting, so perhaps doing without __uint128_t is faster?
  std::sort(
    reinterpret_cast<__uint128_t*>(entries),
    reinterpret_cast<__uint128_t*>(entries + args.logs_count_));
  stop_watch.stop();
  VLOG(0) << "Sorted " << args.logs_count_ << " log entries in " << stop_watch.elapsed_ms() << "ms";

  uint32_t result_count = 1;
  args.output_buffer_[0] = entries[0].get_position();
  for (uint32_t i = 1; i < args.logs_count_; ++i) {
    // compact the logs if the same offset appears in a row, and covers the same data region.
    // because we sorted it by offset and then ordinal, later logs can overwrite the earlier one.
    if (entries[i].get_offset() == entries[i - 1].get_offset()) {
      const log::RecordLogType* prev_p = args.log_buffer_.resolve(entries[i - 1].get_position());
      log::RecordLogType* next_p = args.log_buffer_.resolve(entries[i].get_position());
      if (prev_p->header_.log_type_code_ != next_p->header_.log_type_code_) {
        // increment log can be superseded by overwrite log,
        // overwrite log can be merged with increment log.
        // however, these usecases are probably much less frequent than the following.
        // so, we don't compact this case so far.
      } else if (prev_p->header_.get_type() == log::kLogCodeArrayOverwrite) {
        // two overwrite logs might be compacted
        const ArrayOverwriteLogType* prev = reinterpret_cast<const ArrayOverwriteLogType*>(prev_p);
        const ArrayOverwriteLogType* next = reinterpret_cast<const ArrayOverwriteLogType*>(next_p);
        // is the data region same or superseded?
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
      } else {
        // two increment logs of same type/offset can be merged into one.
        ASSERT_ND(prev_p->header_.get_type() == log::kLogCodeArrayIncrement);
        const ArrayIncrementLogType* prev = reinterpret_cast<const ArrayIncrementLogType*>(prev_p);
        ArrayIncrementLogType* next = reinterpret_cast<ArrayIncrementLogType*>(next_p);
        if (prev->value_type_ == next->value_type_
          && prev->payload_offset_ == next->payload_offset_) {
          // add up the prev's addendum to next, then delete prev.
          next->merge(*prev);
          --result_count;
        }
      }
    }
    args.output_buffer_[result_count] = entries[i].get_position();
    ++result_count;
  }

  stop_watch_entire.stop();
  LOG(INFO) << "Array-" << id_ << " sort_batch() done in  " << stop_watch_entire.elapsed_ms()
      << "ms  for " << args.logs_count_ << " log entries, compacted them to"
        << result_count << " log entries";
  *args.written_count_ = result_count;
}

std::ostream& operator<<(std::ostream& o, const ArrayPartitioner& v) {
  o << "<ArrayPartitioner>";
  if (v.data_) {
    o << "<array_size_>" << v.data_->array_size_ << "</array_size_>"
      << "<bucket_size_>" << v.data_->bucket_size_ << "</bucket_size_>";
    for (uint16_t i = 0; i < kInteriorFanout; ++i) {
      o << "<range bucket=\"" << i << "\" partition=\"" << v.data_->bucket_owners_[i] << "\" />";
    }
  } else {
    o << "Not yet designed";
  }
  o << "</ArrayPartitioner>";
  return o;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
