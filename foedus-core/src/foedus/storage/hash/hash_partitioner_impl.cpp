/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include "foedus/storage/hash/hash_partitioner_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace hash {

HashPartitioner::HashPartitioner(Partitioner* parent)
  : engine_(parent->get_engine()),
    id_(parent->get_storage_id()),
    metadata_(PartitionerMetadata::get_metadata(engine_, id_)) {
  ASSERT_ND(metadata_->mutex_.is_initialized());
  if (metadata_->valid_) {
    data_ = reinterpret_cast<HashPartitionerData*>(metadata_->locate_data(engine_));
  } else {
    data_ = nullptr;
  }
}

const PartitionId* HashPartitioner::get_bucket_owners() const {
  ASSERT_ND(data_);
  return data_->bin_owners_;
}
bool HashPartitioner::is_partitionable() const {
  ASSERT_ND(data_);
  return data_->partitionable_;
}

void design_partition_thread(HashPartitioner* partitioner, uint16_t task) {
  partitioner->design_partition_task(task);
}

ErrorStack HashPartitioner::design_partition(
  const Partitioner::DesignPartitionArguments& /*args*/) {
  ASSERT_ND(metadata_->mutex_.is_initialized());
  ASSERT_ND(data_ == nullptr);
  HashStorage storage(engine_, id_);
  ASSERT_ND(storage.exists());
  HashStorageControlBlock* control_block = storage.get_control_block();

  soc::SharedMutexScope mutex_scope(&metadata_->mutex_);
  ASSERT_ND(!metadata_->valid_);
  HashBin total_bin_count = storage.get_bin_count();
  uint16_t node_count = engine_->get_soc_count();
  uint64_t bytes = HashPartitionerData::object_size(node_count, total_bin_count);
  WRAP_ERROR_CODE(metadata_->allocate_data(engine_, &mutex_scope, bytes));
  data_ = reinterpret_cast<HashPartitionerData*>(metadata_->locate_data(engine_));

  data_->levels_ = storage.get_levels();
  ASSERT_ND(storage.get_levels() >= 1U);
  data_->bin_bits_ = storage.get_bin_bits();
  data_->bin_shifts_ = storage.get_bin_shifts();
  data_->partitionable_ = node_count > 1U;
  data_->total_bin_count_ = total_bin_count;

  if (!data_->partitionable_) {
    // No partitioning needed. We don't even allocate memory for bin_owners_ in this case
    metadata_->valid_ = true;
    return kRetOk;
  }

  ASSERT_ND(!control_block->root_page_pointer_.volatile_pointer_.is_null());

  // simply checks the owner of volatile pointers in last-level intermediate pages.
  // though this is an in-memory task, parallelize to make it even faster.
  // each sub-task is a pointer from the root intermediate page.
  if (storage.get_levels() == 1U) {
    VLOG(0) << "Not worth parallelization. just one level. run it on a single thread";
    for (uint16_t i = 0; i < total_bin_count; ++i) {
      design_partition_task(i);
    }
  } else {
    HashBin interval = kHashMaxBins[storage.get_levels() - 1U];
    ASSERT_ND(interval < total_bin_count);
    ASSERT_ND(interval * kHashIntermediatePageFanout >= total_bin_count);
    std::vector<std::thread> threads;
    for (uint16_t i = 0; interval * i < total_bin_count; ++i) {
      threads.emplace_back(design_partition_thread, this, i);
    }
    LOG(INFO) << "Lanched " << threads.size() << " threads. joining..";

    for (auto& t : threads) {
      t.join();
    }
    LOG(INFO) << "Joined. Designing done";
  }

  metadata_->valid_ = true;
  return kRetOk;
}

void HashPartitioner::design_partition_task(uint16_t task) {
  VLOG(0) << "Task-" << task << " started.";
  ASSERT_ND(data_->partitionable_);
  HashStorage storage(engine_, id_);
  HashStorageControlBlock* control_block = storage.get_control_block();
  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();

  // root page is guaranteed to have volatile version.
  HashIntermediatePage* root_page = reinterpret_cast<HashIntermediatePage*>(
    resolver.resolve_offset(control_block->root_page_pointer_.volatile_pointer_));

  HashBin interval = kHashMaxBins[root_page->get_level()];
  VolatilePagePointer pointer = root_page->get_pointer(task).volatile_pointer_;
  if (pointer.is_null()) {
    VLOG(0) << "Task-" << task << " got an empty task. null volatile pointer";
    std::memset(data_->bin_owners_ + (interval * task), 0, interval);
  } else if (root_page->get_level() == 0) {
    ASSERT_ND(interval = 1ULL);
    ASSERT_ND(storage.get_levels() == 1U);
    VLOG(2) << "Task-" << task << " is trivial.";
    data_->bin_owners_[task] = pointer.components.numa_node;
  } else {
    HashIntermediatePage* child
      = reinterpret_cast<HashIntermediatePage*>(resolver.resolve_offset(pointer));
    design_partition_task_recurse(resolver, child);
  }

  VLOG(0) << "Task-" << task << " ended.";
}

void HashPartitioner::design_partition_task_recurse(
  const memory::GlobalVolatilePageResolver& resolver,
  const HashIntermediatePage* page) {
  ASSERT_ND(page->header().get_page_type() == kHashIntermediatePageType);
  HashBin interval = kHashMaxBins[page->get_level()];
  HashBin begin = page->get_bin_range().begin_;
  for (uint16_t i = 0; begin + i * interval < data_->total_bin_count_; ++i) {
    HashBin sub_begin = begin + i * interval;
    VolatilePagePointer pointer = page->get_pointer(i).volatile_pointer_;
    if (pointer.is_null()) {
      std::memset(data_->bin_owners_ + begin + (interval * i), 0, interval);
    } else if (page->get_level() == 0) {
      ASSERT_ND(interval = 1ULL);
      data_->bin_owners_[sub_begin] = pointer.components.numa_node;
    } else {
      HashIntermediatePage* child
        = reinterpret_cast<HashIntermediatePage*>(resolver.resolve_offset(pointer));
      design_partition_task_recurse(resolver, child);
    }
  }
}


void HashPartitioner::partition_batch(const Partitioner::PartitionBatchArguments& args) const {
  if (!is_partitionable()) {
    std::memset(args.results_, 0, sizeof(PartitionId) * args.logs_count_);
    return;
  }

  HashStorage storage(engine_, id_);
  uint8_t bin_shifts = storage.get_bin_shifts();
  for (uint32_t i = 0; i < args.logs_count_; ++i) {
    const HashCommonLogType *log = reinterpret_cast<const HashCommonLogType*>(
      args.log_buffer_.resolve(args.log_positions_[i]));
    log->assert_type();
    ASSERT_ND(log->header_.storage_id_ == id_);
    // wanna SIMD-ize this... most of the cost here comes from this function.
    HashValue hash = hashinate(log->get_key(), log->key_length_);
    HashBin bin = hash >> bin_shifts;
    ASSERT_ND(bin < storage.get_bin_count());
    args.results_[i] = data_->bin_owners_[bin];
  }
}

/**
  * Used in sort_batch().
  * \li 0-5 bytes: HashBin, the most significant.
  * \li 6-7 bytes: compressed epoch (difference from base_epoch)
  * \li 8-11 bytes: in-epoch-ordinal
  * \li 12-15 bytes: BufferPosition (doesn't have to be sorted together, but for simplicity)
  * Be careful on endian! We use uint128_t to make it easier and faster.
  */
struct SortEntry {
  inline void set(
    HashBin                   bin,
    uint16_t                  compressed_epoch,
    uint32_t                  in_epoch_ordinal,
    snapshot::BufferPosition  position) ALWAYS_INLINE {
    ASSERT_ND(bin < (1ULL << kHashMaxBinBits));
    data_
      = static_cast<__uint128_t>(bin) << 80
        | static_cast<__uint128_t>(compressed_epoch) << 64
        | static_cast<__uint128_t>(in_epoch_ordinal) << 32
        | static_cast<__uint128_t>(position);
  }
  inline HashBin get_bin() const ALWAYS_INLINE {
    return static_cast<HashBin>(data_ >> 80);
  }
  inline snapshot::BufferPosition get_position() const ALWAYS_INLINE {
    return static_cast<snapshot::BufferPosition>(data_);
  }
  __uint128_t data_;
};

/** subroutine of sort_batch */
// __attribute__ ((noinline))  // was useful to forcibly show it on cpu profile. nothing more.
void prepare_sort_entries(
  uint8_t bin_shifts,
  const Partitioner::SortBatchArguments& args,
  SortEntry* entries) {
  // CPU profile of partition_hash_perf: ??%.
  const Epoch base_epoch = args.base_epoch_;
  for (uint32_t i = 0; i < args.logs_count_; ++i) {
    const HashCommonLogType* log_entry = reinterpret_cast<const HashCommonLogType*>(
      args.log_buffer_.resolve(args.log_positions_[i]));
    log_entry->assert_type();
    Epoch epoch = log_entry->header_.xct_id_.get_epoch();
    ASSERT_ND(epoch.subtract(base_epoch) < (1U << 16));
    uint16_t compressed_epoch = epoch.subtract(base_epoch);
    // this is expensive.. should keep hash in log entries
    HashValue hash = hashinate(log_entry->get_key(), log_entry->key_length_);
    entries[i].set(
      hash >> bin_shifts,
      compressed_epoch,
      log_entry->header_.xct_id_.get_ordinal(),
      args.log_positions_[i]);
  }
}

/** subroutine of sort_batch */
// __attribute__ ((noinline))  // was useful to forcibly show it on cpu profile. nothing more.
uint32_t compact_logs(
  uint8_t /*bin_shifts*/,
  const Partitioner::SortBatchArguments& args,
  SortEntry* /*entries*/) {
  // TASK(Hideaki) mapper side compaction.
  // Unlike array, we have to consider all combinations of insert/delete/overwrite.
  // Also needs to exactly compare keys. We probably need to store hashes in log to make it worth.
  return args.logs_count_;
/*
  // CPU profile of partition_hash_perf: ??%.
  uint32_t result_count = 1;
  args.output_buffer_[0] = entries[0].get_position();
  HashBin prev_bin = entries[0].get_bin();
  for (uint32_t i = 1; i < args.logs_count_; ++i) {
    // compact the logs if the same offset appears in a row, and covers the same data region.
    // because we sorted it by offset and then ordinal, later logs can overwrite the earlier one.
    HashBin cur_bin = entries[i].get_bin();
    if (UNLIKELY(cur_bin == prev_bin)) {
      const log::RecordLogType* prev_p = args.log_buffer_.resolve(entries[i - 1].get_position());
      log::RecordLogType* next_p = args.log_buffer_.resolve(entries[i].get_position());
      if (prev_p->header_.log_type_code_ != next_p->header_.log_type_code_) {
        // increment log can be superseded by overwrite log,
        // overwrite log can be merged with increment log.
        // however, these usecases are probably much less frequent than the following.
        // so, we don't compact this case so far.
      } else if (prev_p->header_.get_type() == log::kLogCodeHashOverwrite) {
        // two overwrite logs might be compacted
        const HashOverwriteLogType* prev = reinterpret_cast<const HashOverwriteLogType*>(prev_p);
        const HashOverwriteLogType* next = reinterpret_cast<const HashOverwriteLogType*>(next_p);
        // is the data region same or superseded?
        uint16_t prev_begin = prev->payload_offset_;
        uint16_t prev_end = prev_begin + prev->payload_count_;
        uint16_t next_begin = next->payload_offset_;
        uint16_t next_end = next_begin + next->payload_count_;
        if (next_begin <= prev_begin && next_end >= prev_end) {
          --result_count;
        }

        // the logic checks data range against only the previous entry.
        // we might have a situation where 3 or more log entries have the same hash offset
        // and the data regions are like following
        // Log 1: [4, 8) bytes, Log 2: [8, 12) bytes, Log 3: [4, 8) bytes
        // If we check further, Log 3 can eliminate Log 1. However, the check is expensive..
      } else {
        // two increment logs of same type/offset can be merged into one.
        ASSERT_ND(prev_p->header_.get_type() == log::kLogCodeHashIncrement);
        const HashIncrementLogType* prev = reinterpret_cast<const HashIncrementLogType*>(prev_p);
        HashIncrementLogType* next = reinterpret_cast<HashIncrementLogType*>(next_p);
        if (prev->value_type_ == next->value_type_
          && prev->payload_offset_ == next->payload_offset_) {
          // add up the prev's addendum to next, then delete prev.
          next->merge(*prev);
          --result_count;
        }
      }
    } else {
      prev_bin = cur_bin;
    }
    args.output_buffer_[result_count] = entries[i].get_position();
    ++result_count;
  }
  return result_count;
  */
}

void HashPartitioner::sort_batch(const Partitioner::SortBatchArguments& args) const {
  if (args.logs_count_ == 0) {
    *args.written_count_ = 0;
    return;
  }

  // we so far sort them in one path.
  // to save memory, we could do multi-path merge-sort.
  // however, in reality each log has many bytes, so log_count is not that big.
  args.work_memory_->assure_capacity(sizeof(SortEntry) * args.logs_count_);

  debugging::StopWatch stop_watch_entire;

  ASSERT_ND(sizeof(SortEntry) == 16U);
  SortEntry* entries = reinterpret_cast<SortEntry*>(args.work_memory_->get_block());
  prepare_sort_entries(data_->bin_shifts_, args, entries);

  debugging::StopWatch stop_watch;
  // Gave up non-gcc support because of aarch64 support. yes, we can also assume __uint128_t.
  // CPU profile of partition_hash_perf: ??% (introsort_loop) + ??% (other inlined).
  std::sort(
    reinterpret_cast<__uint128_t*>(entries),
    reinterpret_cast<__uint128_t*>(entries + args.logs_count_));
  stop_watch.stop();
  VLOG(0) << "Sorted " << args.logs_count_ << " log entries in " << stop_watch.elapsed_ms() << "ms";

  uint32_t result_count = compact_logs(data_->bin_shifts_, args, entries);

  stop_watch_entire.stop();
  VLOG(0) << "Hash-" << id_ << " sort_batch() done in  " << stop_watch_entire.elapsed_ms()
      << "ms  for " << args.logs_count_ << " log entries, compacted them to"
        << result_count << " log entries";
  *args.written_count_ = result_count;
}

std::ostream& operator<<(std::ostream& o, const HashPartitioner& v) {
  o << "<HashPartitioner>";
  if (v.data_) {
    o << "<levels_>" << static_cast<int>(v.data_->levels_) << "</levels_>"
      << "<total_bin_count_>" << v.data_->total_bin_count_ << "</total_bin_count_>";
  } else {
    o << "Not yet designed";
  }
  o << "</HashPartitioner>";
  return o;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
