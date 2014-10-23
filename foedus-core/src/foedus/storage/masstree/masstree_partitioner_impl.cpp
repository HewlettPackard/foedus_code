/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <ostream>
#include <string>
#include <utility>

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
      for (MasstreeIntermediatePointerIterator it(vol); it.is_valid(); it.next()) {
        data_->low_keys_[data_->partition_count_] = it.get_low_key();
        const DualPagePointer& pointer = it.get_pointer();
        uint16_t assignment;
        if (!pointer.volatile_pointer_.is_null()) {
          Page* page = resolver.resolve_offset(pointer.volatile_pointer_);
          assignment = page->get_header().stat_last_updater_node_;
        } else {
          SnapshotPagePointer pointer = it.get_pointer().snapshot_pointer_;
          assignment = extract_numa_node_from_snapshot_pointer(pointer);
        }
        data_->partitions_[data_->partition_count_] = assignment;
        ++data_->partition_count_;
      }
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

#if 0
/**
 * Unlike array's sort entry, this doesn't contain key itself except the first 8 byte.
 * Keys are arbitrary lengthes, so we just point to outside memory.
 * In case this causes too much overhead, we store first slice in this object.
 * If this is not enough to differentiate most of keys, it will cause cache miss.
 * @todo not used so far. let's try this if the following code turns out to be slow.
 */
struct SortEntry {
  /** First 8-byte slice of key. infimum if key_length==0 (so, can simply use it in all cases).*/
  KeySlice                  first_slice_;
  /** Points to part of log_buffer rather than containing the key itself */
  const char*               key_;
  uint16_t                  key_length_;
  /** compressed epoch (difference from base_epoch) */
  uint16_t                  compressed_epoch_;
  /** in-epoch-ordinal */
  uint16_t                  in_epoch_ordinal_;
  snapshot::BufferPosition  position_;
};
#endif  // 0

void MasstreePartitioner::sort_batch(const Partitioner::SortBatchArguments& args) const {
  debugging::StopWatch stop_watch_entire;

  // Unlike array's sort_batch, we don't do any advanced optimization here.
  // Keys are arbitrary lengthes, so we have to anyway follow pointers.
  // Thus, we do a super-simple sort by following BufferPosition everytime.
  // If this causes too much overhead, let's store a fixed number of slices as sort entries.
  struct Comparator {
    explicit Comparator(const snapshot::LogBuffer& log_buffer) : log_buffer_(log_buffer) {}
    /** less than operator */
    bool operator() (snapshot::BufferPosition left, snapshot::BufferPosition right) const {
      ASSERT_ND(left != right);
      const MasstreeCommonLogType* left_rec = resolve_log(log_buffer_, left);
      const MasstreeCommonLogType* right_rec = resolve_log(log_buffer_, right);
      return MasstreeCommonLogType::compare_key_and_xct_id(left_rec, right_rec) < 0;
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
  VLOG(0) << "Masstree-" << id_ << " sort_batch() done in  " << stop_watch_entire.elapsed_ms()
      << "ms  for " << args.logs_count_ << " log entries";
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
