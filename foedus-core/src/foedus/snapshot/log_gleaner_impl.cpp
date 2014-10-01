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
#include "foedus/storage/composer.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"

namespace foedus {
namespace snapshot {

LogGleaner::LogGleaner(Engine* engine, Snapshot* snapshot, SnapshotManagerPimpl* manager)
  : LogGleanerRef(engine), snapshot_(snapshot), manager_(manager) {
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
  uint16_t node_count = engine_->get_options().thread_.group_count_;
  for (uint16_t node = 0; node < node_count; ++node) {
    LogReducerRef reducer(engine_, node);
    reducer.clear();
  }
}

ErrorStack LogGleaner::execute() {
  LOG(INFO) << "gleaner_thread_ starts running: " << *this;
  clear_all();

  // Request each node's snapshot manager to launch mappers/reducers threads
  control_block_->gleaning_ = true;
  control_block_->snapshot_id_ = snapshot_->id_;
  control_block_->base_epoch_ = snapshot_->base_epoch_.value();
  control_block_->valid_until_epoch_ = snapshot_->valid_until_epoch_.value();
  manager_->control_block_->wakeup_snapshot_children();

  // then, wait until all mappers/reducers are done
  while (!is_error() && !is_all_completed()) {
    // snapshot is an infrequent operation, doesn't have to wake up immediately.
    // just sleep for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  control_block_->gleaning_ = false;

  if (is_error()) {
    LOG(ERROR) << "Some mapper/reducer got an error. " << *this;
  } else if (!is_all_completed()) {
    LOG(WARNING) << "gleaner stopped without completion. cancelled? " << *this;
  } else {
    LOG(INFO) << "All mappers/reducers successfully done. Now on to the final phase." << *this;
    CHECK_ERROR(construct_root_pages());
  }

  LOG(INFO) << "gleaner stopping.. cancelling reducers and mappers: " << *this;
  CHECK_ERROR(cancel_reducers_mappers());
  ASSERT_ND(is_all_exitted());
  LOG(INFO) << "gleaner ends: " << *this;

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
    buffer_sizes.push_back(reducers_[i]->get_root_info_page_count());
    buffers.push_back(reinterpret_cast<const storage::Page*>(
      reducers_[i]->get_root_info_buffer().get_block()));
  }

  // reuse the first reducer's work memory
  memory::AlignedMemorySlice work_memory(&reducers_[0]->get_composer_work_memory());
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

    std::unique_ptr< storage::Composer > composer(reducers_[0]->create_composer(min_storage_id));
    storage::SnapshotPagePointer new_root_page_pointer;
    CHECK_ERROR(composer->construct_root(
      &tmp_array[0],
      input_count,
      work_memory,
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

  ASSERT_ND(new_root_page_pointers_.size() == get_partitioner_count());
  stop_watch.stop();
  LOG(INFO) << "constructed root pages for " << new_root_page_pointers_.size()
    << " storages. (partitioner_count=" << get_partitioner_count() << "). "
    << " in " << stop_watch.elapsed_ms() << "ms. "<< *this;
  return kRetOk;
}

const storage::Partitioner* LogGleaner::get_or_create_partitioner(storage::StorageId storage_id) {
  {
    std::lock_guard<std::mutex> guard(partitioners_mutex_);
    auto it = partitioners_.find(storage_id);
    if (it != partitioners_.end()) {
      return it->second;
    }
  }

  // not found, let's create a new one, but out of the critical section to avoid contention.
  storage::Partitioner* partitioner = storage::Partitioner::create_partitioner(engine_, storage_id);
  {
    std::lock_guard<std::mutex> guard(partitioners_mutex_);
    auto it = partitioners_.find(storage_id);
    if (it != partitioners_.end()) {
      // oh, someone has just added it!
      delete partitioner;
      return it->second;
    } else {
      partitioners_.insert(std::pair<storage::StorageId, storage::Partitioner*>(
        storage_id, partitioner));
      return partitioner;
    }
  }
}


std::string LogGleaner::to_string() const {
  std::stringstream stream;
  stream << *this;
  return stream.str();
}
std::ostream& operator<<(std::ostream& o, const LogGleaner& v) {
  o << "<LogGleaner>"
    << *v.snapshot_
    << "<completed_count_>" << v.control_block_->completed_count_ << "</completed_count_>"
    << "<completed_mapper_count_>"
      << v.control_block_->completed_mapper_count_ << "</completed_mapper_count_>"
    << "<partitioner_count>" << v.get_partitioner_count() << "</partitioner_count>"
    << "<error_count_>" << v.control_block_->error_count_ << "</error_count_>"
    << "<exit_count_>" << v.control_block_->exit_count_ << "</exit_count_>";
  o << "</LogGleaner>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
