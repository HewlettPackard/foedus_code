/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/log_gleaner_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/snapshot/log_mapper_impl.hpp"
#include "foedus/snapshot/log_reducer_impl.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/snapshot/snapshot_manager_pimpl.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"

namespace foedus {
namespace snapshot {

LogGleaner::LogGleaner(Engine* engine, const Snapshot& new_snapshot)
  : LogGleanerRef(engine),
    new_snapshot_(new_snapshot) {
}
LogGleaner::~LogGleaner() {
  if (partitioner_metadata_) {
    for (storage::StorageId i = 0; i <= new_snapshot_.max_storage_id_; ++i) {
      partitioner_metadata_[i].uninitialize();
    }
  }
}

ErrorStack LogGleaner::cancel_reducers_mappers() {
  if (is_all_exitted()) {
    VLOG(0) << "All mappers/reducers have already exitted. " << *this;
    return kRetOk;
  }
  LOG(INFO) << "Requesting all mappers/reducers threads to stop.. " << *this;
  control_block_->cancelled_ = true;
  const uint32_t kTimeoutSleeps = 3000U;
  uint32_t count = 0;
  while (!is_all_exitted()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (++count > kTimeoutSleeps) {
      return ERROR_STACK(kErrorCodeSnapshotExitTimeout);
    }
  }
  return kRetOk;
}

void LogGleaner::clear_all() {
  control_block_->clear_counts();
  control_block_->cur_snapshot_ = new_snapshot_;
  uint16_t node_count = engine_->get_options().thread_.group_count_;
  for (uint16_t node = 0; node < node_count; ++node) {
    LogReducerRef reducer(engine_, node);
    reducer.clear();
  }
  for (storage::StorageId i = 0; i <= new_snapshot_.max_storage_id_; ++i) {
    partitioner_metadata_[i].initialize();
  }
}

ErrorStack LogGleaner::design_partitions() {
  // so far single threaded to debug easily.
  // but, let's prepare for parallelization so that we can switch later.
  ErrorStack result;
  design_partitions_run(1U, new_snapshot_.max_storage_id_, &result);
  return result;
}

void LogGleaner::design_partitions_run(
  storage::StorageId from,
  storage::StorageId count,
  ErrorStack* result) {
  *result = kRetOk;
  LOG(INFO) << "Determining partitions for Storage-" << from << " to " << (from + count - 1) << ".";
  for (storage::StorageId id = from; id < from + count; ++id) {
    storage::Partitioner partitioner(engine_, id);
    ErrorStack ret = partitioner.design_partition();
    if (ret.is_error()) {
      LOG(ERROR) << "Error while determining partitions for storage-" << id << ":" << ret;
      *result = ret;
      break;
    }
  }
  LOG(INFO) << "Determined partitions for Storage-" << from << " to " << (from + count - 1) << ".";
}

ErrorStack LogGleaner::execute() {
  LOG(INFO) << "Gleaner starts running: snapshot_id=" << get_snapshot_id();
  clear_all();

  LOG(INFO) << "Gleaner Step 1: Design partitions for all storages...";
  // Another approach is to delay this step until some mapper really needs it so that we can
  // skip partition-designing for storages that weren't modified.
  // However, it requires synchronization in mapper/reducer and this step is anyway fast enough.
  // So, we so far simply design partitions for all of them.
  CHECK_ERROR(design_partitions());

  LOG(INFO) << "Gleaner Step 2: Run mappers/reducers...";
  // Request each node's snapshot manager to launch mappers/reducers threads
  control_block_->gleaning_ = true;
  engine_->get_soc_manager()->get_shared_memory_repo()->get_global_memory_anchors()->
    snapshot_manager_memory_->wakeup_snapshot_children();

  // then, wait until all mappers/reducers are done
  while (!is_error() && !is_all_completed()) {
    // snapshot is an infrequent operation, doesn't have to wake up immediately.
    // just sleep for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  control_block_->gleaning_ = false;

  LOG(INFO) << "Gleaner Step 3: Combine outputs from reducers (root page info)..." << *this;
  if (is_error()) {
    LOG(ERROR) << "Some mapper/reducer got an error. " << *this;
  } else if (!is_all_completed()) {
    LOG(WARNING) << "gleaner stopped without completion. cancelled? " << *this;
  } else {
    LOG(INFO) << "All mappers/reducers successfully done. Now on to the final phase." << *this;
    CHECK_ERROR(construct_root_pages());
  }

  LOG(INFO) << "Gleaner Step 4: Uninitializing...";
  CHECK_ERROR(cancel_reducers_mappers());
  ASSERT_ND(is_all_exitted());
  LOG(INFO) << "Gleaner ends";
  return kRetOk;
}

ErrorStack LogGleaner::construct_root_pages() {
  ASSERT_ND(new_root_page_pointers_.size() == 0);
  debugging::StopWatch stop_watch;
  const uint16_t count = control_block_->reducers_count_;
  std::vector<const storage::Page*> tmp_array(count, nullptr);
  std::vector<uint16_t> cursors;
  std::vector<uint16_t> buffer_sizes;
  std::vector<const storage::Page*> buffers;
  for (uint16_t i = 0; i < count; ++i) {
    cursors.push_back(0);
    LogReducerRef reducer(engine_, i);
    buffer_sizes.push_back(reducer.get_total_storage_count());
    buffers.push_back(reducer.get_root_info_pages());
  }

  // Combining the root page info doesn't require much memory, so this size should be enough.
  memory::AlignedMemory work_memory;
  work_memory.alloc(1U << 21, 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  memory::AlignedMemorySlice work_memory_slice(&work_memory);

  storage::StorageId prev_storage_id = 0;
  // each reducer's root-info-page must be sorted by storage_id, so we do kind of merge-sort here.
  while (true) {
    // determine which storage to process by finding the smallest storage_id
    storage::StorageId min_storage_id = 0;
    for (uint16_t i = 0; i < count; ++i) {
      if (cursors[i] == buffer_sizes[i]) {
        continue;
      }
      const storage::Page* root_info_page = buffers[i] + cursors[i];
      storage::StorageId storage_id = root_info_page->get_header().storage_id_;
      ASSERT_ND(storage_id > prev_storage_id);
      if (min_storage_id == 0) {
        min_storage_id = storage_id;
      } else {
        min_storage_id = std::min(min_storage_id, storage_id);
      }
    }

    if (min_storage_id == 0) {
      break;  // all reducers' all root info pages processed
    }

    // fill tmp_array
    uint16_t input_count = 0;
    for (uint16_t i = 0; i < count; ++i) {
      if (cursors[i] == buffer_sizes[i]) {
        continue;
      }
      const storage::Page* root_info_page = buffers[i] + cursors[i];
      storage::StorageId storage_id = root_info_page->get_header().storage_id_;
      if (storage_id == min_storage_id) {
        tmp_array[input_count] = root_info_page;
        ++input_count;
      }
    }
    ASSERT_ND(input_count > 0);

    storage::Composer composer(engine_, min_storage_id);
    storage::SnapshotPagePointer new_root_page_pointer;
    CHECK_ERROR(composer.construct_root(
      &tmp_array[0],
      input_count,
      work_memory_slice,
      &new_root_page_pointer));
    ASSERT_ND(new_root_page_pointer > 0);
    ASSERT_ND(new_root_page_pointers_.find(min_storage_id) == new_root_page_pointers_.end());
    new_root_page_pointers_.insert(std::pair<storage::StorageId, storage::SnapshotPagePointer>(
      min_storage_id, new_root_page_pointer));

    // done for this storage. advance cursors
    prev_storage_id = min_storage_id;
    for (uint16_t i = 0; i < count; ++i) {
      if (cursors[i] == buffer_sizes[i]) {
        continue;
      }
      const storage::Page* root_info_page = buffers[i] + cursors[i];
      storage::StorageId storage_id = root_info_page->get_header().storage_id_;
      if (storage_id == min_storage_id) {
        cursors[i] = cursors[i] + 1;
      }
    }
  }

  stop_watch.stop();
  LOG(INFO) << "constructed root pages for " << new_root_page_pointers_.size()
    << " storages. in " << stop_watch.elapsed_ms() << "ms. "<< *this;
  return kRetOk;
}

std::string LogGleaner::to_string() const {
  std::stringstream stream;
  stream << *this;
  return stream.str();
}
std::ostream& operator<<(std::ostream& o, const LogGleaner& v) {
  o << "<LogGleaner>"
    << v.new_snapshot_
    << "<completed_count_>" << v.control_block_->completed_count_ << "</completed_count_>"
    << "<completed_mapper_count_>"
      << v.control_block_->completed_mapper_count_ << "</completed_mapper_count_>"
    << "<error_count_>" << v.control_block_->error_count_ << "</error_count_>"
    << "<exit_count_>" << v.control_block_->exit_count_ << "</exit_count_>";
  o << "</LogGleaner>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
