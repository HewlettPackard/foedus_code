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
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/debugging/rdtsc_watch.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace masstree {


////////////////////////////////////////////////////////////////////////////////
///
///      MasstreePartitioner methods
///
////////////////////////////////////////////////////////////////////////////////
MasstreePartitioner::MasstreePartitioner(Partitioner* parent)
  : engine_(parent->get_engine()),
    id_(parent->get_storage_id()),
    metadata_(PartitionerMetadata::get_metadata(engine_, id_)) {
  if (metadata_->valid_) {
    data_ = reinterpret_cast<MasstreePartitionerData*>(metadata_->locate_data(engine_));
  } else {
    data_ = nullptr;
  }
}

ErrorStack MasstreePartitioner::design_partition(
  const Partitioner::DesignPartitionArguments& args) {
  MasstreeStorage storage(engine_, id_);
  MasstreeStorageControlBlock* control_block = storage.get_control_block();
  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();

  // Read current volatile version and previous snapshot version of the root page
  WRAP_ERROR_CODE(args.work_memory_->assure_capacity(kPageSize * 2));
  Page* buffers = reinterpret_cast<Page*>(args.work_memory_->get_block());
  MasstreeIntermediatePage* vol = reinterpret_cast<MasstreeIntermediatePage*>(buffers);
  MasstreeIntermediatePage* snp = reinterpret_cast<MasstreeIntermediatePage*>(buffers + 1);
  SnapshotPagePointer snapshot_page_id = control_block->root_page_pointer_.snapshot_pointer_;
  MasstreeIntermediatePage* root_volatile = reinterpret_cast<MasstreeIntermediatePage*>(
    resolver.resolve_offset(control_block->root_page_pointer_.volatile_pointer_));
  CHECK_ERROR(read_page_safe(root_volatile, vol));
  if (snapshot_page_id != 0) {
    WRAP_ERROR_CODE(args.snapshot_files_->read_page(snapshot_page_id, snp));
  }

  soc::SharedMutexScope scope(&metadata_->mutex_);  // protect this metadata
  if (metadata_->valid_) {
    // someone has already initialized this??
    LOG(FATAL) << "Masstree-" << id_ << " partition already designed??:" << *this;
  }
  WRAP_ERROR_CODE(metadata_->allocate_data(engine_, &scope, sizeof(MasstreePartitionerData)));
  data_ = reinterpret_cast<MasstreePartitionerData*>(metadata_->locate_data(engine_));

  ASSERT_ND(!vol->is_border());
  if (engine_->get_soc_count() == 1U) {
    // no partitioning needed
    data_->partition_count_ = 1;
    data_->low_keys_[0] = kInfimumSlice;
    data_->partitions_[0] = 0;
  } else {
    // simply the separators in root page is the partition keys.
    // if we already have a snapshot page, we use the same partition keys, though
    // assigned nodes might be changed
    data_->partition_count_ = 0;
    if (snapshot_page_id == 0) {
      CHECK_ERROR(design_partition_first(vol));
    } else {
      // So far same assignment as previous snapshot.
      // TODO(Hideaki) check current volatile pages to change the assignments.
      for (MasstreeIntermediatePointerIterator it(snp); it.is_valid(); it.next()) {
        data_->low_keys_[data_->partition_count_] = it.get_low_key();
        SnapshotPagePointer pointer = it.get_pointer().snapshot_pointer_;
        uint16_t assignment = extract_numa_node_from_snapshot_pointer(pointer);
        data_->partitions_[data_->partition_count_] = assignment;
        ++data_->partition_count_;
      }
    }
  }

  metadata_->valid_ = true;
  LOG(INFO) << "Masstree-" << id_ << std::endl << " partitions:" << *this;
  return kRetOk;
}


void design_partition_first_parallel_recurse(
  const memory::GlobalVolatilePageResolver& resolver,
  const MasstreePage* page,
  uint32_t subtree_id,
  OwnerSamples* result,
  assorted::UniformRandom* unirand) {
  uint32_t node = page->header().stat_last_updater_node_;
  result->increment(node, subtree_id);
  // we don't care foster twins. this is just for sampling.
  if (!page->is_border()) {
    const auto* casted = reinterpret_cast<const MasstreeIntermediatePage*>(page);
    const uint32_t kSamplingWidth = 3;  // follow this many pointers per page.
    for (uint32_t rep = 0; rep < kSamplingWidth; ++rep) {
      uint32_t index = unirand->next_uint32() % (casted->get_key_count() + 1U);
      const MasstreeIntermediatePage::MiniPage& minipage = casted->get_minipage(index);
      uint32_t index_mini = unirand->next_uint32() % (minipage.key_count_ + 1U);
      VolatilePagePointer pointer = minipage.pointers_[index_mini].volatile_pointer_;
      if (pointer.is_null()) {
        // because now this page might be changing, this is possible
        continue;
      }
      MasstreePage* child = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(pointer));
      design_partition_first_parallel_recurse(resolver, child, subtree_id, result, unirand);
    }
  } else {
    // so far do not bother going down to next layer. the border page's owner probably
    // represents it well. it might not, but a worst case always exists (such as just 1 page
    // in the first layer.. in that case anyway no luck).
  }
}

void design_partition_first_parallel(
  Engine* engine,
  VolatilePagePointer subtree,
  uint32_t subtree_id,
  OwnerSamples* result) {
  const memory::GlobalVolatilePageResolver& resolver
    = engine->get_memory_manager()->get_global_volatile_page_resolver();
  ASSERT_ND(subtree_id < result->subtrees_);
  debugging::StopWatch watch;
  MasstreePage* page = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(subtree));
  assorted::UniformRandom unirand(subtree_id);
  design_partition_first_parallel_recurse(resolver, page, subtree_id, result, &unirand);
  watch.stop();
  VLOG(0) << "Subtree-" << subtree_id << " done in " << watch.elapsed_us() << "us";
}


ErrorStack MasstreePartitioner::design_partition_first(const MasstreeIntermediatePage* root) {
  LOG(INFO) << "Initial partition design for Masstree-" << id_;
  // randomly sample descendant pages to determine the assignment.
  // we parallelize this step for each pointer in the root page
  data_->partition_count_ = 0;
  std::vector<VolatilePagePointer> pointers;
  pointers.reserve(kMaxIntermediatePointers);
  for (MasstreeIntermediatePointerIterator it(root); it.is_valid(); it.next()) {
    data_->low_keys_[data_->partition_count_] = it.get_low_key();
    const DualPagePointer& pointer = it.get_pointer();
    ASSERT_ND(!pointer.volatile_pointer_.is_null());  // because this is first snapshot.
    pointers.push_back(pointer.volatile_pointer_);
    data_->partitions_[data_->partition_count_] = 0;
    ++data_->partition_count_;
  }

  ASSERT_ND(data_->partition_count_ == pointers.size());
  LOG(INFO) << "Launching " << data_->partition_count_ << " threads to take random samples..";
  OwnerSamples samples(engine_->get_soc_count(), data_->partition_count_);
  std::vector< std::thread > threads;
  threads.reserve(data_->partition_count_);
  for (uint32_t subtree_id = 0; subtree_id < data_->partition_count_; ++subtree_id) {
    threads.emplace_back(
      design_partition_first_parallel,
      engine_,
      pointers[subtree_id],
      subtree_id,
      &samples);
  }

  LOG(INFO) << "Launched. Joining..";
  for (auto& t : threads) {
    t.join();
  }

  samples.assign_owners();
  LOG(INFO) << "Joined. Results:" << samples;
  for (uint32_t subtree_id = 0; subtree_id < data_->partition_count_; ++subtree_id) {
    data_->partitions_[subtree_id] = samples.get_assignment(subtree_id);
  }

  return kRetOk;
}

void OwnerSamples::assign_owners() {
  // so far simply the node that has majority. but we might want to balance out
  for (uint32_t subtree_id = 0; subtree_id < subtrees_; ++subtree_id) {
    uint32_t max_node = 0;
    uint32_t max_count = at(0, subtree_id);
    for (uint32_t node = 1; node < nodes_; ++node) {
      if (at(node, subtree_id) > max_count) {
        max_node = node;
        max_count = at(node, subtree_id);
      }
    }
    assignments_[subtree_id] = max_node;
  }
}

std::ostream& operator<<(std::ostream& o, const OwnerSamples& v) {
  o << "<OwnerSamples nodes=\"" << v.nodes_
    << "\" subtrees=\"" << v.subtrees_ << "\">" << std::endl;
  o << "<!-- legend id=\"x\" assignment=\"x\">";
  for (uint32_t node = 0; node < v.nodes_; ++node) {
    o << "[Node-" << node << "] ";
  }
  o << " -->" << std::endl;
  for (uint32_t subtree_id = 0; subtree_id < v.subtrees_; ++subtree_id) {
    o << "  <subtree id=\"" << subtree_id
      << "\" assignment=\"" << v.get_assignment(subtree_id) << "\">";
    for (uint32_t node = 0; node < v.nodes_; ++node) {
      o << assorted::Hex(v.at(node, subtree_id), 6) << " ";
    }
    o << "</subtree>" << std::endl;
  }
  o << std::endl << "</OwnerSamples>";
  return o;
}

ErrorStack MasstreePartitioner::read_page_safe(MasstreePage* src, MasstreePage* out) {
  SPINLOCK_WHILE(true) {
    // a modifying user transaction increments version counter before it unlocks the page,
    // so the following protocol assures that there happened nothing.
    uint32_t before = src->header().page_version_.get_version_counter();
    assorted::memory_fence_acquire();
    bool locked_before = src->header().page_version_.is_locked();
    assorted::memory_fence_acquire();
    std::memcpy(out, src, kPageSize);
    assorted::memory_fence_acquire();
    uint32_t after = src->header().page_version_.get_version_counter();
    assorted::memory_fence_acquire();
    bool locked_after = src->header().page_version_.is_locked();
    assorted::memory_fence_acquire();
    uint32_t again = src->header().page_version_.get_version_counter();
    if (locked_before || locked_after) {
      VLOG(0) << "Interesting, observed locked page during OCC-read in partition designer. retry";
      assorted::spinlock_yield();  // snapshot is not in rush. let other threads move on.
      continue;
    } else if (before == after && after == again) {
      // as far as it's a consistent read, the current status of the page doesn't matter.
      // pointers/records were valid at least until recently, so it's safe to follow them.
      break;
    }
    VLOG(0) << "Interesting, version conflict during OCC-read in partition designer. retry";
  }
  return kRetOk;
}

bool MasstreePartitioner::is_partitionable() const {
  return data_->partition_count_ > 1U;
}

void MasstreePartitioner::partition_batch(const Partitioner::PartitionBatchArguments& args) const {
  debugging::RdtscWatch stop_watch;
  if (!is_partitionable()) {
    std::memset(args.results_, 0, sizeof(PartitionId) * args.logs_count_);
    return;
  }

  for (uint32_t i = 0; i < args.logs_count_; ++i) {
    const MasstreeCommonLogType* rec = resolve_log(args.log_buffer_, args.log_positions_[i]);
    uint16_t key_length = rec->key_length_;
    const char* key = rec->get_key();
    args.results_[i] = data_->find_partition(key, key_length);
  }
  stop_watch.stop();
  VLOG(0) << "Masstree-:" << id_ << " took " << stop_watch.elapsed() << "cycles"
    << " to partition " << args.logs_count_ << " entries. #partitions=" << data_->partition_count_;
  // if these binary searches are too costly, let's optimize them.
}

void MasstreePartitioner::sort_batch_general(const Partitioner::SortBatchArguments& args) const {
  debugging::StopWatch stop_watch_entire;

  // Unlike array's sort_batch, we don't do any advanced optimization here.
  // Keys are arbitrary lengthes, so we have to anyway follow pointers.
  // Thus, we do a super-simple sort by following BufferPosition everytime.
  // If this causes too much overhead, let's store a fixed number of slices as sort entries.
  struct Comparator {
    explicit Comparator(const snapshot::LogBuffer& log_buffer) : log_buffer_(log_buffer) {}
    /** less than operator */
    inline bool operator() (
      snapshot::BufferPosition left,
      snapshot::BufferPosition right) const ALWAYS_INLINE {
      ASSERT_ND(left != right);
      const MasstreeCommonLogType* left_rec = resolve_log(log_buffer_, left);
      const MasstreeCommonLogType* right_rec = resolve_log(log_buffer_, right);
      int cmp = MasstreeCommonLogType::compare_logs(left_rec, right_rec);
      return (cmp < 0 || (cmp == 0 && left < right));
    }
    const snapshot::LogBuffer& log_buffer_;
  };

  std::memcpy(
    args.output_buffer_,
    args.log_positions_,
    args.logs_count_ * sizeof(snapshot::BufferPosition));
  Comparator comparator(args.log_buffer_);
  std::sort(args.output_buffer_, args.output_buffer_ + args.logs_count_, comparator);

  // No compaction for masstree yet. Anyway this method is not optimized
  *args.written_count_ = args.logs_count_;
  stop_watch_entire.stop();
  VLOG(0) << "Masstree-" << id_ << " sort_batch_general() done in  "
      << stop_watch_entire.elapsed_ms() << "ms  for " << args.logs_count_ << " log entries. "
      << " shortest_key=" << args.shortest_key_length_
      << " longest_key=" << args.longest_key_length_;
}

/* Left for future reference (yes, yes, this should be moved to documents rather than code)
This one was way slower than the uint128_t + std::sort below. 8-10 Mlogs/sec/core -> 2.5M
as far as we cause L1 miss for each comparison, it's basically same as the general func above.

void MasstreePartitioner::sort_batch_8bytes(const Partitioner::SortBatchArguments& args) const {
  ASSERT_ND(args.shortest_key_length_ == sizeof(KeySlice));
  ASSERT_ND(args.longest_key_length_ == sizeof(KeySlice));

  debugging::StopWatch stop_watch_entire;

  // Only a slightly different comparator that exploits the fact that all keys are slices
  struct SliceComparator {
    explicit SliceComparator(const snapshot::LogBuffer& log_buffer) : log_buffer_(log_buffer) {}
    bool operator() (snapshot::BufferPosition left, snapshot::BufferPosition right) const {
      ASSERT_ND(left != right);
      const MasstreeCommonLogType* left_rec = resolve_log(log_buffer_, left);
      const MasstreeCommonLogType* right_rec = resolve_log(log_buffer_, right);

      KeySlice left_slice = normalize_be_bytes_full_aligned(left_rec->get_key());
      KeySlice right_slice = normalize_be_bytes_full_aligned(right_rec->get_key());
      if (left_slice != right_slice) {
        return left_slice < right_slice;
      }
      int cmp = left_rec->header_.xct_id_.compare_epoch_and_orginal(right_rec->header_.xct_id_);
      if (cmp != 0) {
        return cmp < 0;
      }
      return left < right;
    }
    const snapshot::LogBuffer& log_buffer_;
  };

  std::memcpy(
    args.output_buffer_,
    args.log_positions_,
    args.logs_count_ * sizeof(snapshot::BufferPosition));
  SliceComparator comparator(args.log_buffer_);
  std::sort(args.output_buffer_, args.output_buffer_ + args.logs_count_, comparator);

  *args.written_count_ = args.logs_count_;
  stop_watch_entire.stop();
  VLOG(0) << "Masstree-" << id_ << " sort_batch_8bytes() done in  "
      << stop_watch_entire.elapsed_ms() << "ms  for " << args.logs_count_ << " log entries. "
      << " shortest_key=" << args.shortest_key_length_
      << " longest_key=" << args.longest_key_length_;
}
*/

// typedef uint32_t LogIndex;

/**
 * Unlike array's sort entry, we don't always use this because keys are arbitrary lengthes.
 * We use this when all keys are up to 8 bytes.
 * To speed up other cases, we might want to use this for more than 8 bytes, later, later..
 *
 * We couldn't fit this within 16 bytes.
 * We tried to do it by substituting log position with 3 byte "index" of input logs, but then
 * the final output has to be retrieved with lots of L1 cache misses, making it slower.
 * After all, this is 10%-20% slower than an incorrect code that assumes in-epoch-orginal
 * is within 2-bytes and everything fits 16 bytes, using uint128_t. ah, sweet, but no.
 */
struct SortEntry {
  inline void set(
    KeySlice first_slice,
    uint16_t compressed_epoch,
    uint32_t in_epoch_ordinal,
    snapshot::BufferPosition position) ALWAYS_INLINE {
    first_slice_ = first_slice;
    combined_epoch_ = (static_cast<uint64_t>(compressed_epoch) << 32) | in_epoch_ordinal;
    position_ = position;
  }
  inline bool operator<(const SortEntry& rhs) const ALWAYS_INLINE {
    if (first_slice_ != rhs.first_slice_) {
      return first_slice_ < rhs.first_slice_;
    }
    if (combined_epoch_ != rhs.combined_epoch_) {
      return combined_epoch_ < rhs.combined_epoch_;
    }
    return position_ < rhs.position_;
  }

  KeySlice first_slice_;
  uint64_t combined_epoch_;  // compressed_epoch_ << 32 | in_epoch_ordinal_
  snapshot::BufferPosition position_;
  uint32_t dummy2_;
  // so unfortunate that this doesn't fit in 16 bytes.
  // because it's now 24 bytes and not 16b aligned, we can't use uint128_t either.
};

/** subroutine of sort_batch_8bytes */
// __attribute__ ((noinline))  // was useful to forcibly show it on cpu profile. nothing more.
void retrieve_positions(
  uint32_t logs_count,
  const SortEntry* entries,
  snapshot::BufferPosition* out) {
  // CPU profile of partition_masstree_perf: 2-3%. (10-15% if the "index" idea is used)
  for (uint32_t i = 0; i < logs_count; ++i) {
    out[i] = entries[i].position_;
  }
}

/** subroutine of sort_batch_8bytes */
// __attribute__ ((noinline))  // was useful to forcibly show it on cpu profile. nothing more.
void prepare_sort_entries(const Partitioner::SortBatchArguments& args, SortEntry* entries) {
  const Epoch base_epoch = args.base_epoch_;
  // CPU profile of partition_masstree_perf: 9-10%.
  for (uint32_t i = 0; i < args.logs_count_; ++i) {
    const MasstreeCommonLogType* log_entry = reinterpret_cast<const MasstreeCommonLogType*>(
      args.log_buffer_.resolve(args.log_positions_[i]));
    ASSERT_ND(log_entry->header_.log_type_code_ == log::kLogCodeMasstreeInsert
      || log_entry->header_.log_type_code_ == log::kLogCodeMasstreeDelete
      || log_entry->header_.log_type_code_ == log::kLogCodeMasstreeOverwrite);
    ASSERT_ND(log_entry->key_length_ == sizeof(KeySlice));
    Epoch epoch = log_entry->header_.xct_id_.get_epoch();
    ASSERT_ND(epoch.subtract(base_epoch) < (1U << 16));
    uint16_t compressed_epoch = epoch.subtract(base_epoch);
    entries[i].set(
      normalize_be_bytes_full_aligned(log_entry->get_key()),
      compressed_epoch,
      log_entry->header_.xct_id_.get_ordinal(),
      args.log_positions_[i]);
  }
}

void MasstreePartitioner::sort_batch_8bytes(const Partitioner::SortBatchArguments& args) const {
  ASSERT_ND(args.shortest_key_length_ == sizeof(KeySlice));
  ASSERT_ND(args.longest_key_length_ == sizeof(KeySlice));
  args.work_memory_->assure_capacity(sizeof(SortEntry) * args.logs_count_);

  debugging::StopWatch stop_watch_entire;
  ASSERT_ND(sizeof(SortEntry) == 24U);
  SortEntry* entries = reinterpret_cast<SortEntry*>(args.work_memory_->get_block());
  prepare_sort_entries(args, entries);

  // CPU profile of partition_masstree_perf: 80% (introsort_loop) + 9% (other inlined parts).
  std::sort(entries, entries + args.logs_count_);

  retrieve_positions(args.logs_count_, entries, args.output_buffer_);
  *args.written_count_ = args.logs_count_;
  stop_watch_entire.stop();
  VLOG(0) << "Masstree-" << id_ << " sort_batch_8bytes() done in  "
      << stop_watch_entire.elapsed_ms() << "ms  for " << args.logs_count_ << " log entries. "
      << " shortest_key=" << args.shortest_key_length_
      << " longest_key=" << args.longest_key_length_;
}


void MasstreePartitioner::sort_batch(const Partitioner::SortBatchArguments& args) const {
  if (args.logs_count_ == 0) {
    *args.written_count_ = 0;
    return;
  }

  if (args.longest_key_length_ == sizeof(KeySlice)
      && args.shortest_key_length_ == sizeof(KeySlice)) {
    sort_batch_8bytes(args);
  } else {
    sort_batch_general(args);
  }
}

std::ostream& operator<<(std::ostream& o, const MasstreePartitioner& v) {
  o << "<MasstreePartitioner>";
  if (v.data_) {
    o << "<partition_count_>" << v.data_->partition_count_ << "</partition_count_>"
      << "<partitions>";
    for (uint16_t i = 0; i < v.data_->partition_count_; ++i) {
      o << std::endl << "  <partition node=\"" << v.data_->partitions_[i] << "\">"
        << assorted::Hex(v.data_->low_keys_[i], 16)
        << "</partition>";
    }
    o << "</partitions>";
  } else {
    o << "Not yet designed";
  }
  o << "</MasstreePartitioner>";
  return o;
}


////////////////////////////////////////////////////////////////////////////////
///
///      MasstreePartitionerData methods, binary search
///
////////////////////////////////////////////////////////////////////////////////
uint16_t MasstreePartitionerData::find_partition(const char* key, uint16_t key_length) const {
  if (key_length == 0) {
    return partitions_[0];
  }

  // so far we do a simple sequential search here. we might want
  // 1) binary search until candidate count becomes less than 8, 2) sequential search.
  ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));
  KeySlice slice = normalize_be_bytes_full_aligned(key);
  uint16_t i;
  for (i = 1; i < partition_count_; ++i) {
    if (low_keys_[i] > slice) {
      break;
    }
  }
  // now, i points to the first partition whose low_key is strictly larger than key.
  // thus, the one before it should be the right partition.
  return partitions_[i - 1];
}


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
