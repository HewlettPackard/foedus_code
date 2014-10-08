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
///      Local utility functions
///
////////////////////////////////////////////////////////////////////////////////
inline std::string append_slice_to_prefix(const std::string& prefix, KeySlice slice) {
  char appendix[sizeof(slice)];
  assorted::write_bigendian<KeySlice>(slice, appendix);
  return prefix + std::string(appendix, sizeof(slice));
}

inline std::string get_full_key(
  const MasstreeBorderPage& page,
  uint32_t rec,
  const std::string& prefix) {
  KeySlice slice = page.get_slice(rec);
  char slice_be[sizeof(slice)];
  assorted::write_bigendian<KeySlice>(slice, slice_be);

  uint16_t remaining = page.get_remaining_key_length(rec);
  uint16_t suffix_length = page.get_suffix_length(rec);

  std::string ret;
  ret.reserve(prefix.length() + remaining);
  ret.append(prefix);
  ret.append(std::string(slice_be, remaining >= 8U ? 8U : remaining));
  if (suffix_length > 0) {
    ret.append(std::string(page.get_record(rec), suffix_length));
  }
  ASSERT_ND(ret.size() == prefix.length() + remaining);
  return ret;
}

/** Returns negative, 0, positive if left<right, left==right, left>right. */
inline int compare_keys(
  const char* left,
  uint16_t left_length,
  const char* right,
  uint16_t right_length) {
  uint16_t cmp_length = std::min(left_length, right_length);
  int result = std::memcmp(left, right, cmp_length);
  if (result != 0) {
    return result;
  } else if (left_length < right_length) {
    return -1;
  } else if (left_length > right_length) {
    return 1;
  } else {
    return 0;
  }
}

inline const char* extract_log_key(const log::RecordLogType* rec, uint16_t* key_length) {
  log::LogCode type = static_cast<log::LogCode>(rec->header_.log_type_code_);
  switch (type) {
  case log::kLogCodeMasstreeInsert:
    *key_length = reinterpret_cast<const MasstreeInsertLogType*>(rec)->key_length_;
    return reinterpret_cast<const MasstreeInsertLogType*>(rec)->data_;
  case log::kLogCodeMasstreeDelete:
    *key_length = reinterpret_cast<const MasstreeDeleteLogType*>(rec)->key_length_;
    return reinterpret_cast<const MasstreeDeleteLogType*>(rec)->data_;
  case log::kLogCodeMasstreeOverwrite:
    *key_length = reinterpret_cast<const MasstreeOverwriteLogType*>(rec)->key_length_;
    return reinterpret_cast<const MasstreeOverwriteLogType*>(rec)->data_;
  default:
    LOG(FATAL) << "Unexpected log type for masstree storage:" << rec->header_;
    return nullptr;
  }
}


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
  memory::AlignedMemory* work_memory,
  cache::SnapshotFileSet* snapshot_files) {
  // Almost all implementation details are in MasstreePartitionerInDesignData
  MasstreePartitionerInDesignData context(engine_, id_, work_memory, snapshot_files);
  CHECK_ERROR(context.initialize());
  CHECK_ERROR(context.enumerate());
  CHECK_ERROR(context.design());
  uint32_t required_size = context.get_output_size();
  required_size = assorted::align64(required_size);

  // let's allocate this size in partitioner data. we need to take mutex for this
  {
    PartitionerMetadata* size_metadata = PartitionerMetadata::get_index0_metadata(engine_);
    soc::SharedMutexScope size_scope(&size_metadata->mutex_);
    WRAP_ERROR_CODE(metadata_->allocate_data(engine_, &size_scope, required_size));
  }
  data_ = reinterpret_cast<MasstreePartitionerData*>(metadata_->locate_data(engine_));
  LOG(INFO) << "Allocated " << required_size << " bytes for partitioner data of Masstree-" << id_;

  context.copy_output(data_);
  LOG(INFO) << "Masstree-" << id_ << " partitions:" << *this;
  return kRetOk;
}
uint64_t MasstreePartitioner::get_required_design_buffer_size() const {
  uint32_t desired_branches
    = engine_->get_soc_count() * MasstreePartitioner::kPartitionThresholdPerNode;
  return 2ULL * kPageSize * desired_branches;
}

bool MasstreePartitioner::is_partitionable() const {
  return data_->partition_count_ > 1U;
}

void MasstreePartitioner::partition_batch(
  PartitionId                     /*local_partition*/,
  const snapshot::LogBuffer&      log_buffer,
  const snapshot::BufferPosition* log_positions,
  uint32_t                        logs_count,
  PartitionId*                    results) const {
  debugging::RdtscWatch stop_watch;
  for (uint32_t i = 0; i < logs_count; ++i) {
    const log::RecordLogType* rec = log_buffer.resolve(log_positions[i]);
    uint16_t key_length;
    const char* key = extract_log_key(rec, &key_length);
    results[i] = data_->find_partition(key, key_length);
  }
  stop_watch.stop();
  VLOG(0) << "Masstree-:" << id_ << " took " << stop_watch.elapsed() << "cycles"
    << " to partition " << logs_count << " entries. #partitions=" << data_->partition_count_;
  // if these binary searches are too costly, let's optimize them.
}

/**
 * Unlike array's sort entry, this doesn't contain key itself.
 * Keys are arbitrary lengthes, so we just point to outside memory.
 * If this causes too much overhead, let's store some number of slices in this object.
 * Either way, way more expensive than array...
 */
struct SortEntry {
  /** Points to part of log_buffer rather than containing the key itself */
  const char*               key_;
  uint16_t                  key_length_;
  /** compressed epoch (difference from base_epoch) */
  uint16_t                  compressed_epoch_;
  /** in-epoch-ordinal */
  uint16_t                  in_epoch_ordinal_;
  snapshot::BufferPosition  position_;
};

// this doesn't use work memory so far.
uint64_t MasstreePartitioner::get_required_sort_buffer_size(uint32_t /*log_count*/) const {
  return 0;
}
void MasstreePartitioner::sort_batch(
  const snapshot::LogBuffer&        log_buffer,
  const snapshot::BufferPosition*   log_positions,
  uint32_t                          logs_count,
  const memory::AlignedMemorySlice& /* sort_buffer*/,
  Epoch                             /* base_epoch */,
  snapshot::BufferPosition*         output_buffer,
  uint32_t*                         written_count) const {
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
      const log::RecordLogType* left_rec = log_buffer_.resolve(left);
      const log::RecordLogType* right_rec = log_buffer_.resolve(right);
      uint16_t left_key_length, right_key_length;
      const char* left_key = extract_log_key(left_rec, &left_key_length);
      const char* right_key = extract_log_key(right_rec, &right_key_length);
      int cmp = compare_keys(left_key, left_key_length, right_key, right_key_length);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }

      // if the key is same, we have to compare epoch and in_epoch_ordinal_
      Epoch left_epoch = left_rec->header_.xct_id_.get_epoch();
      Epoch right_epoch = right_rec->header_.xct_id_.get_epoch();
      ASSERT_ND(left_epoch.is_valid());
      ASSERT_ND(right_epoch.is_valid());
      if (left_epoch < right_epoch) {
        return true;
      } else if (left_epoch > right_epoch) {
        return false;
      }

      // if the epoch is the same, compare in_epoch_ordinal_.
      uint32_t left_ordinal = left_rec->header_.xct_id_.get_ordinal();
      uint32_t right_ordinal = right_rec->header_.xct_id_.get_ordinal();
      if (left_ordinal < right_ordinal) {
        return true;
      } else if (left_ordinal > right_ordinal) {
        return false;
      }

      // if all of them are the same, this must be log entries of one transaction on same key.
      // in that case the log position tells chronological order.
      return left < right;
    }
    const snapshot::LogBuffer& log_buffer_;
  };

  std::memcpy(output_buffer, log_positions, logs_count * sizeof(snapshot::BufferPosition));
  Comparator comparator(log_buffer);
  std::sort(output_buffer, output_buffer + logs_count, comparator);

  // No compaction for masstree yet. Anyway this method is not optimized
  *written_count = logs_count;
  stop_watch_entire.stop();
  VLOG(0) << "Masstree-" << id_ << " sort_batch() done in  " << stop_watch_entire.elapsed_ms()
      << "ms  for " << logs_count << " log entries";
}

std::ostream& operator<<(std::ostream& o, const MasstreePartitioner& v) {
  o << "<MasstreePartitioner>";
  if (v.data_) {
    o << "<partition_count_>" << v.data_->partition_count_ << "</partition_count_>"
      << "<partitions>";
    for (uint16_t i = 0; i < v.data_->partition_count_; ++i) {
      const MasstreePartitionerData::PartitionHeader& partition = v.data_->partitions_[i];
      o << std::endl << "  <partition key_offset=\"" << partition.key_offset_
        << "\" key_length=\"" << partition.key_length_
        << "\" node=\"" << partition.key_length_ << "\">"
        << assorted::HexString(std::string(v.data_->get_partition_key(i), partition.key_length_))
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
    return partitions_[0].node_;
  }

  // so far we do a simple binary search here. without any 8 bytes integer comparison or
  // batching. if this turns out to be the bottleneck, let's optimize it.

  // basically this is what std::upper_bound does.
  uint16_t count = partition_count_;
  uint16_t low = 0;
  while (count > 0) {
    uint16_t step = count / 2U;
    uint16_t mid = low + step;
    int cmp = compare_keys(key, key_length, get_partition_key(mid), partitions_[mid].key_length_);

    if (cmp == 0) {
      // then definitely the key is in mid. low_key is inclusive
      return partitions_[mid].node_;
    } else if (cmp > 0) {
        low = mid + 1U;
        count -= step + 1U;
    } else {
      count = step;
    }
  }
  ASSERT_ND(low > 0);
  // now, low points to the first partition whose low_key is strictly larger than key.
  // thus, the one before it should be the right partition.
  // low cannot be 0 because the first one is always "".
  return partitions_[low - 1].node_;
}


////////////////////////////////////////////////////////////////////////////////
///
///      MasstreePartitionerInDesignData and partition-designing methods
///
////////////////////////////////////////////////////////////////////////////////
MasstreePartitionerInDesignData::MasstreePartitionerInDesignData(
  Engine* engine,
  StorageId id,
  memory::AlignedMemory* work_memory,
  cache::SnapshotFileSet* snapshot_files)
  : engine_(engine),
    id_(id),
    storage_(engine, id),
    work_memory_(work_memory),
    snapshot_files_(snapshot_files),
    desired_branches_(
      engine_->get_soc_count() * MasstreePartitioner::kPartitionThresholdPerNode),
    volatile_resolver_(engine_->get_memory_manager()->get_global_volatile_page_resolver()) {
  ASSERT_ND(storage_.exists());
}

ErrorStack MasstreePartitionerInDesignData::initialize() {
  if (work_memory_->get_size() < 2ULL * kPageSize * desired_branches_) {
    return ERROR_STACK_MSG(kErrorCodeInvalidParameter, "work_memory too small");
  }
  tmp_pages_.attach(
    reinterpret_cast<memory::PagePoolControlBlock*>(tmp_pages_control_block_),
    work_memory_->get_block(),
    work_memory_->get_size(),
    true);
  CHECK_ERROR(tmp_pages_.initialize());
  return kRetOk;
}

MasstreePartitionerInDesignData::~MasstreePartitionerInDesignData() {
  COERCE_ERROR(tmp_pages_.uninitialize());  // this never returns an error
}

ErrorStack MasstreePartitionerInDesignData::enumerate() {
  // Initial page is the root page
  CHECK_ERROR(push_branch_page(storage_.get_control_block()->root_page_pointer_, ""));

  LOG(INFO) << "Partition-design for Masstree-" << storage_.get_id() << "(" << storage_.get_name()
    << "). Enumerating branch pages...";

  // digg down each page until we find enough branch pages or there are no more pages
  while (!enumerated_pages_.empty() && get_branch_count() < desired_branches_) {
    CHECK_ERROR(pop_and_explore_branch_page());
  }

  LOG(INFO) << "Masstree-" << storage_.get_id() << " enumerated " << enumerated_pages_.size()
    << " branch pages and " << enumerated_records_.size() << " branch records.";
  return kRetOk;
}

ErrorStack MasstreePartitionerInDesignData::design() {
  designed_partitions_.clear();
  // First, order all enumerated pages/records by key. simply std::map
  std::map<std::string, uint16_t> sorted;  // <low_key, owner_node>
  while (!enumerated_pages_.empty()) {
    BranchPage popped = enumerated_pages_.front();
    MasstreePage* tmp_page = resolve_tmp_page(popped.tmp_page_offset_);
    std::string low_key = append_slice_to_prefix(popped.prefix_, tmp_page->get_low_fence());
    if (sorted.find(low_key) != sorted.end()) {
      LOG(ERROR) << "Masstree-" << storage_.get_id() << " has dup entries in enumerated_pages_"
        << " with the same key: " << assorted::HexString(low_key);
      return ERROR_STACK(kErrorCodeInternalError);
    }
    sorted.insert(std::pair<std::string, uint16_t>(low_key, popped.owner_node_));
  }
  for (const BranchRecord& record : enumerated_records_) {
    if (sorted.find(record.record_key_) != sorted.end()) {
      LOG(ERROR) << "Masstree-" << storage_.get_id() << " has duplicate entries in"
        << " enumerated_records_ with the same key: " << assorted::HexString(record.record_key_);
      return ERROR_STACK(kErrorCodeInternalError);
    }
    sorted.insert(std::pair<std::string, uint16_t>(record.record_key_, record.owner_node_));
  }

  // assure the beginning entry.
  if (sorted.find("") == sorted.end()) {
    sorted.insert(std::pair<std::string, uint16_t>("", 0));
  }

  // convert the key ranges to partitions.
  // we so far blindly inherit what owner_node says in the input.
  // this might cause imbalance, but let's do advanced things later.
  // we only remove redundant ranges with same owner_node contiguously
  Partition current;
  auto it = sorted.cbegin();
  ASSERT_ND(it->first == std::string(""));
  current.low_key_ = it->first;
  current.owner_node_ = it->second;
  for (; it != sorted.cend(); ++it) {
    if (it->second == current.owner_node_) {
      continue;  // skip redundant range.
    }
    designed_partitions_.emplace_back(current);
    current.low_key_ = it->first;
    current.owner_node_ = it->second;
  }
  designed_partitions_.emplace_back(current);
  LOG(INFO) << "Masstree-" << storage_.get_id() << " generated " << designed_partitions_.size()
    << " partitions";
  return kRetOk;
}

uint32_t MasstreePartitionerInDesignData::get_output_size() const {
  uint32_t ret = 8;  // partition_count_ and padding_
  // region for partion
  ret += 8 * designed_partitions_.size();
  // region for low key
  for (const Partition& partition : designed_partitions_) {
    ret += partition.low_key_.size();
  }
  return ret;
}

void MasstreePartitionerInDesignData::copy_output(MasstreePartitionerData* destination) const {
  destination->partition_count_ = designed_partitions_.size();
  uint32_t current_offset = 0;
  char* key_region = destination->get_key_region();
  for (uint32_t i = 0; i < designed_partitions_.size(); ++i) {
    const Partition& partition = designed_partitions_[i];
    destination->partitions_[i].key_length_ = partition.low_key_.size();
    destination->partitions_[i].key_offset_ = current_offset;
    destination->partitions_[i].node_ = partition.owner_node_;
    std::memcpy(key_region + current_offset, partition.low_key_.data(), partition.low_key_.size());
    current_offset += partition.low_key_.size();
  }
}

MasstreePage* MasstreePartitionerInDesignData::resolve_tmp_page(memory::PagePoolOffset offset) {
  // the real page offset is different from that of this temporary pool, so use
  // resolve_offset_newpage to bypass assertion.
  return reinterpret_cast<MasstreePage*>(tmp_pages_.get_resolver().resolve_offset_newpage(offset));
}

ErrorStack MasstreePartitionerInDesignData::push_branch_page(
  const DualPagePointer& ptr,
  const std::string& prefix) {
  memory::PagePoolOffset offset;
  WRAP_ERROR_CODE(tmp_pages_.grab_one(&offset));
  CHECK_ERROR(read_page(ptr, offset));

  // so far the owner of the node is the node that has recently updated the page.
  // this is not an atomically/transactionally maintained info, but enough for partitioning.
  MasstreePage* tmp_page = resolve_tmp_page(offset);
  uint16_t page_owner = tmp_page->header().stat_last_updater_node_;
  enumerated_pages_.emplace(prefix, offset, page_owner);
  return kRetOk;
}

ErrorStack MasstreePartitionerInDesignData::pop_and_explore_branch_page() {
  ASSERT_ND(!enumerated_pages_.empty());
  BranchPage popped = enumerated_pages_.front();
  MasstreePage* page = resolve_tmp_page(popped.tmp_page_offset_);
  uint32_t key_count = page->get_key_count();
  if (page->is_border()) {
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(page);
    for (uint32_t i = 0; i < key_count; ++i) {
      if (casted->does_point_to_layer(i)) {
        // go in to next layer. prefix now has 8 more bytes
        std::string prefix(append_slice_to_prefix(popped.prefix_, casted->get_slice(i)));
        CHECK_ERROR(push_branch_page(*casted->get_next_layer(i), prefix));
      } else {
        // this is just one record. consider that this covers from this key to next key.
        std::string record_key(get_full_key(*casted, i, popped.prefix_));
        // we assume the record is simply owned by the page.
        enumerated_records_.emplace_back(record_key, popped.owner_node_);
      }
    }
  } else {
    // in an intermediate page, everything is a pointer with the same prefix. simpler.
    MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(page);
    for (uint32_t i = 0; i <= key_count; ++i) {
      const MasstreeIntermediatePage::MiniPage& minipage = casted->get_minipage(i);
      uint8_t mini_count = minipage.key_count_;
      for (uint8_t j = 0; j <= mini_count; ++j) {
        CHECK_ERROR(push_branch_page(minipage.pointers_[j], popped.prefix_));
      }
    }
  }

  // the popped page is no longer needed. release it.
  tmp_pages_.release_one(popped.tmp_page_offset_);
  return kRetOk;
}
ErrorStack MasstreePartitionerInDesignData::read_page(
  const DualPagePointer& ptr,
  memory::PagePoolOffset offset) {
  ASSERT_ND(offset > 0);
  MasstreePage* tmp_page = resolve_tmp_page(offset);
  if (ptr.volatile_pointer_.is_null()) {
    ASSERT_ND(ptr.snapshot_pointer_ != 0);
    // if volatile page doesn't exist, we just follow a snapshot pointer, and we can do so
    // without worrying about concurrent modification.
    WRAP_ERROR_CODE(snapshot_files_->read_page(ptr.snapshot_pointer_, tmp_page));
  } else {
    // if it's a volatile page, we might be reading a half-updated image.
    // as we don't maintain read-sets like usual transactions, we instead do the optimistic
    // read protocol here.
    Page* page = volatile_resolver_.resolve_offset(ptr.volatile_pointer_);
    SPINLOCK_WHILE(true) {
      // a modifying user transaction increments version counter before it unlocks the page,
      // so the following protocol assures that there happened nothing.
      uint32_t before = page->get_header().page_version_.get_version_counter();
      assorted::memory_fence_acquire();
      bool locked_before = page->get_header().page_version_.is_locked();
      assorted::memory_fence_acquire();
      std::memcpy(tmp_page, page, kPageSize);
      assorted::memory_fence_acquire();
      uint32_t after = page->get_header().page_version_.get_version_counter();
      assorted::memory_fence_acquire();
      bool locked_after = page->get_header().page_version_.is_locked();
      assorted::memory_fence_acquire();
      uint32_t again = page->get_header().page_version_.get_version_counter();
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
  }
  return kRetOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
