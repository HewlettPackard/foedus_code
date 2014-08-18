/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/log/log_manager_pimpl.hpp"

#include <glog/logging.h>

#include <string>
#include <thread>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/log/log_options.hpp"
#include "foedus/log/logger_impl.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace log {
ErrorStack LogManagerPimpl::initialize_once() {
  groups_ = engine_->get_options().thread_.group_count_;
  loggers_per_node_ = engine_->get_options().log_.loggers_per_node_;
  const LoggerId total_loggers = loggers_per_node_ * groups_;
  const uint16_t total_threads = engine_->get_options().thread_.get_total_thread_count();
  LOG(INFO) << "Initializing LogManager. #loggers_per_node=" << loggers_per_node_
    << ", #NUMA-nodes=" << static_cast<int>(groups_) << ", #total_threads=" << total_threads;
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_savepoint_manager().is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }
  // see comments in LogOptions#log_paths_
  if (total_loggers == 0 || total_loggers % groups_ != 0 || total_threads % total_loggers != 0
    || total_loggers > total_threads) {
    return ERROR_STACK(kErrorCodeLogInvalidLoggerCount);
  }

  // Initialize durable_global_epoch_
  durable_global_epoch_ = engine_->get_savepoint_manager().get_savepoint_fast().
    get_durable_epoch().value();
  LOG(INFO) << "durable_global_epoch_=" << get_durable_global_epoch();

  // evenly distribute loggers to NUMA nodes, then to cores.
  const uint16_t cores_per_logger = total_threads / total_loggers;
  LoggerId current_logger_id = 0;
  for (thread::ThreadGroupId group = 0; group < groups_; ++group) {
    memory::ScopedNumaPreferred numa_scope(group);
    thread::ThreadLocalOrdinal current_ordinal = 0;
    for (auto j = 0; j < loggers_per_node_; ++j) {
      std::vector< thread::ThreadId > assigned_thread_ids;
      for (auto k = 0; k < cores_per_logger; ++k) {
        assigned_thread_ids.push_back(thread::compose_thread_id(group, current_ordinal));
        current_ordinal++;
      }
      std::string folder = engine_->get_options().log_.convert_folder_path_pattern(group, j);
      // to avoid race, create the root log folder now.
      fs::Path path(folder);
      if (!fs::exists(path)) {
        fs::create_directories(path);
      }
      Logger* logger = new Logger(engine_, current_logger_id, group, j,
                    fs::Path(folder), assigned_thread_ids);
      CHECK_OUTOFMEMORY(logger);
      loggers_.push_back(logger);
      ++current_logger_id;
    }
    ASSERT_ND(current_ordinal == engine_->get_options().thread_.thread_count_per_group_);
  }
  ASSERT_ND(current_logger_id == total_loggers);
  ASSERT_ND(current_logger_id == loggers_.size());

  // call initialize() of each logger.
  // this might take long, so do it in parallel.
  std::vector<std::thread> init_threads;
  for (thread::ThreadGroupId group = 0; group < groups_; ++group) {
    memory::ScopedNumaPreferred numa_scope(group);
    for (auto j = 0; j < loggers_per_node_; ++j) {
      Logger* logger = loggers_[group * loggers_per_node_ + j];
      init_threads.push_back(std::thread([logger]() {
        COERCE_ERROR(logger->initialize());  // TODO(Hideaki) collect errors
      }));
    }
  }
  LOG(INFO) << "Launched threads to initialize loggers. waiting..";
  for (auto& init_thread : init_threads) {
    init_thread.join();
  }
  LOG(INFO) << "All loggers were initialized!";
  return kRetOk;
}

ErrorStack LogManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing LogManager..";
  ErrorStackBatch batch;
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_savepoint_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  batch.uninitialize_and_delete_all(&loggers_);
  return SUMMARIZE_ERROR_BATCH(batch);
}
void LogManagerPimpl::wakeup_loggers() {
  for (Logger* logger : loggers_) {
    logger->wakeup();
  }
}

ErrorStack LogManagerPimpl::refresh_global_durable_epoch() {
  assorted::memory_fence_acquire();
  Epoch min_durable_epoch;
  ASSERT_ND(!min_durable_epoch.is_valid());
  for (Logger* logger : loggers_) {
    min_durable_epoch.store_min(logger->get_durable_epoch());
  }
  ASSERT_ND(min_durable_epoch.is_valid());

  if (min_durable_epoch <= get_durable_global_epoch()) {
    VLOG(0) << "durable_global_epoch_ not advanced";
    return kRetOk;
  }

  VLOG(0) << "Global durable epoch is about to advance from " << get_durable_global_epoch()
    << " to " << min_durable_epoch;
  {
    std::lock_guard<std::mutex> guard(durable_global_epoch_savepoint_mutex_);
    if (min_durable_epoch <= get_durable_global_epoch()) {
      VLOG(0) << "oh, I lost the race.";
      return kRetOk;
    }

    CHECK_ERROR(engine_->get_savepoint_manager().take_savepoint(min_durable_epoch));

    durable_global_epoch_advanced_.notify_all([this, min_durable_epoch]{
      durable_global_epoch_ = min_durable_epoch.value();
    });
  }
  return kRetOk;
}


ErrorCode LogManagerPimpl::wait_until_durable(Epoch commit_epoch, int64_t wait_microseconds) {
  assorted::memory_fence_acquire();
  if (commit_epoch <= get_durable_global_epoch()) {
    DVLOG(1) << "Already durable. commit_epoch=" << commit_epoch << ", durable_global_epoch_="
      << get_durable_global_epoch();
    return kErrorCodeOk;
  }

  if (wait_microseconds == 0) {
    DVLOG(1) << "Conditional check: commit_epoch=" << commit_epoch << ", durable_global_epoch_="
      << get_durable_global_epoch();
    return kErrorCodeTimeout;
  }

  std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
  std::chrono::high_resolution_clock::time_point until
    = now + std::chrono::microseconds(wait_microseconds);
  // @spinlock, but with sleep (not frequently called)
  SPINLOCK_WHILE(commit_epoch > get_durable_global_epoch()) {
    for (Logger* logger : loggers_) {
      logger->wakeup_for_durable_epoch(commit_epoch);
    }
    if (wait_microseconds > 0) {
      VLOG(0) << "Synchronously waiting for commit_epoch " << commit_epoch;
      if (!durable_global_epoch_advanced_.wait_until(until)) {
        LOG(WARNING) << "Timeout occurs. wait_microseconds=" << wait_microseconds;
        return kErrorCodeTimeout;
      }
    } else {
      // here we use a version without predicate. it's fine as we have the while loop outside.
      durable_global_epoch_advanced_.wait();
    }
  }

  VLOG(0) << "durable epoch advanced. durable_global_epoch_=" << get_durable_global_epoch();
  return kErrorCodeOk;
}


void LogManagerPimpl::copy_logger_states(savepoint::Savepoint* new_savepoint) {
  new_savepoint->current_log_files_.clear();
  new_savepoint->oldest_log_files_offset_begin_.clear();
  new_savepoint->current_log_files_.clear();
  new_savepoint->current_log_files_offset_durable_.clear();
  for (Logger* logger : loggers_) {
    logger->copy_logger_state(new_savepoint);
  }
}

}  // namespace log
}  // namespace foedus
