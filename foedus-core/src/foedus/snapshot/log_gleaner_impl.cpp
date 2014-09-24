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

ErrorStack LogGleaner::initialize_once() {
  LOG(INFO) << "Initializing Log Gleaner";
  clear_counts();
  // 2MB ought to be enough for everyone
  nonrecord_log_buffer_.alloc(1 << 21, 1 << 21, memory::AlignedMemory::kNumaAllocInterleaved, 0);
  ASSERT_ND(!nonrecord_log_buffer_.is_null());

  const EngineOptions& options = engine_->get_options();
  const thread::ThreadGroupId numa_nodes = options.thread_.group_count_;
  for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
    memory::ScopedNumaPreferred numa_scope(node);
    for (uint16_t ordinal = 0; ordinal < options.log_.loggers_per_node_; ++ordinal) {
      log::LoggerId logger_id = options.log_.loggers_per_node_ * node + ordinal;
      mappers_.push_back(new LogMapper(engine_, this, logger_id, node));
    }

    reducers_.push_back(new LogReducer(engine_, this, node));
  }

  new_root_page_pointers_.clear();
  return kRetOk;
}

ErrorStack LogGleaner::uninitialize_once() {
  LOG(INFO) << "Uninitializing Log Gleaner";
  ErrorStackBatch batch;
  // note: at this point, mappers_/reducers_ are most likely already stopped (unless there were
  // unexpected errors). We do it again to make sure.
  batch.uninitialize_and_delete_all(&mappers_);
  batch.uninitialize_and_delete_all(&reducers_);
  nonrecord_log_buffer_.release_block();

  new_root_page_pointers_.clear();
  for (std::map<storage::StorageId, storage::Partitioner*>::iterator it = partitioners_.begin();
      it != partitioners_.end(); ++it) {
    delete it->second;
  }
  partitioners_.clear();
  return SUMMARIZE_ERROR_BATCH(batch);
}

bool LogGleaner::is_stop_requested() const {
  return engine_->get_snapshot_manager()->get_pimpl()->is_stop_requested();
}
void LogGleaner::wakeup() {
  engine_->get_snapshot_manager()->get_pimpl()->wakeup();
}

void LogGleaner::cancel_mappers() {
  // first, request to stop all of them before waiting for them.
  LOG(INFO) << "Requesting mappers to stop.. " << *this;
  for (LogMapper* mapper : mappers_) {
    if (mapper->is_initialized()) {
      mapper->request_stop();
    } else {
      LOG(WARNING) << "This mapper is not initilized.. During error handling?" << *mapper;
    }
  }

  LOG(INFO) << "Requested mappers to stop. Now blocking.." << *this;
  for (LogMapper* mapper : mappers_) {
    if (mapper->is_initialized()) {
      mapper->wait_for_stop();
      mapper->uninitialize();
    }
  }
  LOG(INFO) << "All mappers stopped." << *this;
}
void LogGleaner::cancel_reducers() {
  // first, request to stop all of them before waiting for them.
  LOG(INFO) << "Requesting reducers to stop.. " << *this;
  for (LogReducer* reducer : reducers_) {
    if (reducer->is_initialized()) {
      reducer->request_stop();
    } else {
      LOG(WARNING) << "This reducer is not initilized.. During error handling?" << *reducer;
    }
  }

  LOG(INFO) << "Requested reducers to stop. Now blocking.." << *this;
  for (LogReducer* reducer : reducers_) {
    if (reducer->is_initialized()) {
      reducer->wait_for_stop();
      reducer->uninitialize();
    }
  }
  LOG(INFO) << "All reducers stopped." << *this;
}

ErrorStack LogGleaner::execute() {
  LOG(INFO) << "gleaner_thread_ starts running: " << *this;
  clear_counts();

  // initialize mappers and reducers. This launches the threads.
  for (LogMapper* mapper : mappers_) {
    CHECK_ERROR(mapper->initialize());
  }
  for (LogReducer* reducer : reducers_) {
    CHECK_ERROR(reducer->initialize());
  }
  // Wait for completion of mapper/reducer initialization.
  LOG(INFO) << "Waiting for completion of mappers and reducers init.. " << *this;

  // the last one will wake me up.
  while (!is_stop_requested()) {
    engine_->get_snapshot_manager()->get_pimpl()->sleep_a_while();
    ASSERT_ND(ready_to_start_count_ <= mappers_.size() + reducers_.size());
    if (is_all_ready_to_start()) {
      break;
    }
  }

  LOG(INFO) << "Initialized mappers and reducers: " << *this;

  // now let's start!
  start_processing_.signal();

  // then, wait until all mappers/reducers are done
  bool terminated_mappers = false;
  while (error_count_ == 0) {
    if (is_stop_requested() || is_all_completed()) {
      break;
    }
    engine_->get_snapshot_manager()->get_pimpl()->sleep_a_while();
    if (!terminated_mappers && is_all_mappers_completed()) {
      // as soon as all mappers complete, uninitialize them to release unused memories.
      // the last phase of reducers consume lots of resource, so this might help a bit.
      LOG(INFO) << "All mappers are done. Let's immediately release their resources...: " << *this;
      cancel_mappers();
      terminated_mappers = true;
    }
  }

  if (error_count_ > 0) {
    LOG(ERROR) << "Some mapper/reducer got an error. " << *this;
  } else if (!is_all_completed()) {
    LOG(WARNING) << "gleaner_thread_ stopped without completion. cancelled? " << *this;
  } else {
    LOG(INFO) << "All mappers/reducers successfully done. Now on to the final phase." << *this;
    CHECK_ERROR(construct_root_pages());
  }

  LOG(INFO) << "gleaner_thread_ stopping.. cancelling reducers and mappers: " << *this;
  cancel_reducers_mappers();
  ASSERT_ND(exit_count_.load() == mappers_.size() + reducers_.size());
  LOG(INFO) << "gleaner_thread_ ends: " << *this;

  return kRetOk;
}

ErrorStack LogGleaner::construct_root_pages() {
  ASSERT_ND(new_root_page_pointers_.size() == 0);
  debugging::StopWatch stop_watch;
  const uint16_t count = reducers_.size();
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

void LogGleaner::add_nonrecord_log(const log::LogHeader* header) {
  ASSERT_ND(header->get_kind() != log::kEngineLogs || header->get_kind() != log::kStorageLogs);
  uint64_t begins_at = nonrecord_log_buffer_pos_.fetch_add(header->log_length_);
  // we so far assume nonrecord_log_buffer_ is always big enough.
  // TODO(Hideaki) : automatic growing if needed. Very rare, though.
  ASSERT_ND(begins_at + header->log_length_ <= nonrecord_log_buffer_.get_size());
  std::memcpy(
    static_cast<char*>(nonrecord_log_buffer_.get_block()) + begins_at,
    header,
    header->log_length_);
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
    << "<ready_to_start_count_>" << v.ready_to_start_count_ << "</ready_to_start_count_>"
    << "<completed_count_>" << v.completed_count_ << "</completed_count_>"
    << "<completed_mapper_count_>" << v.completed_mapper_count_ << "</completed_mapper_count_>"
    << "<partitioner_count>" << v.get_partitioner_count() << "</partitioner_count>"
    << "<error_count_>" << v.error_count_ << "</error_count_>"
    << "<exit_count_>" << v.exit_count_ << "</exit_count_>"
    << "<nonrecord_log_buffer_pos_>"
      << v.nonrecord_log_buffer_pos_ << "</nonrecord_log_buffer_pos_>";
  o << "<Mappers>";
  for (auto mapper : v.mappers_) {
    o << *mapper;
  }
  o << "</Mappers>";
  o << "<Reducers>";
  for (auto reducer : v.reducers_) {
    o << *reducer;
  }
  o << "</Reducers>";
  o << "</LogGleaner>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
