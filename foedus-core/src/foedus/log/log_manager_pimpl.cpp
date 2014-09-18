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
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace log {
LogManagerPimpl::LogManagerPimpl(Engine* engine) : engine_(engine), control_block_(nullptr) {}

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

  // attach control block
  soc::SharedMemoryRepo* memory_repo = engine_->get_soc_manager().get_shared_memory_repo();
  control_block_ = memory_repo->get_global_memory_anchors()->log_manager_memory_;

  // attach logger_refs
  const uint16_t cores_per_logger = total_threads / total_loggers;
  for (thread::ThreadGroupId group = 0; group < groups_; ++group) {
    soc::NodeMemoryAnchors* node_anchors = memory_repo->get_node_memory_anchors(group);
    for (uint16_t j = 0; j < loggers_per_node_; ++j) {
      LoggerControlBlock* logger_block = node_anchors->logger_memories_[j];
      logger_refs_.emplace_back(LoggerRef(engine_, logger_block));
    }
  }

  if (engine_->is_master()) {
    // In master, we initialize the control block. No local loggers.
    // Initialize durable_global_epoch_
    control_block_->initialize();
    control_block_->durable_global_epoch_
      = engine_->get_savepoint_manager().get_initial_durable_epoch().value();
    LOG(INFO) << "durable_global_epoch_=" << get_durable_global_epoch();
  } else {
    // In SOC, we don't have to initialize the control block, but have to launch local loggers.
    // evenly distribute loggers to NUMA nodes, then to cores.
    soc::SocId node = engine_->get_soc_id();
    thread::ThreadLocalOrdinal current_ordinal = 0;
    soc::NodeMemoryAnchors* node_anchors = memory_repo->get_node_memory_anchors(node);
    for (uint16_t j = 0; j < loggers_per_node_; ++j) {
      LoggerControlBlock* logger_block = node_anchors->logger_memories_[j];
      std::vector< thread::ThreadId > assigned_thread_ids;
      for (auto k = 0; k < cores_per_logger; ++k) {
        assigned_thread_ids.push_back(thread::compose_thread_id(node, current_ordinal));
        current_ordinal++;
      }
      std::string folder = engine_->get_options().log_.convert_folder_path_pattern(node, j);
      // to avoid race, create the root log folder now.
      fs::Path path(folder);
      if (!fs::exists(path)) {
        fs::create_directories(path);
      }
      Logger* logger = new Logger(
        engine_,
        logger_block,
        node * loggers_per_node_ + j,
        node,
        j,
        fs::Path(folder),
        assigned_thread_ids);
      CHECK_OUTOFMEMORY(logger);
      loggers_.push_back(logger);
    }
    ASSERT_ND(current_ordinal == engine_->get_options().thread_.thread_count_per_group_);

    // call initialize() of each logger.
    // this might take long, so do it in parallel.
    std::vector<std::thread> init_threads;
    for (auto j = 0; j < loggers_per_node_; ++j) {
      Logger* logger = loggers_[node * loggers_per_node_ + j];
      init_threads.push_back(std::thread([logger]() {
        COERCE_ERROR(logger->initialize());  // TODO(Hideaki) collect errors
      }));
    }
    LOG(INFO) << "Launched threads to initialize loggers in node-" << node << ". waiting..";
    for (auto& init_thread : init_threads) {
      init_thread.join();
    }
    LOG(INFO) << "All loggers in node-" << node << " were initialized!";
  }

  return kRetOk;
}

ErrorStack LogManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing LogManager..";
  ErrorStackBatch batch;
  if (!engine_->get_thread_pool().is_initialized()
    || !engine_->get_savepoint_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  if (engine_->is_master()) {
    ASSERT_ND(loggers_.empty());
  }
  batch.uninitialize_and_delete_all(&loggers_);
  logger_refs_.clear();
  if (engine_->is_master()) {
    control_block_->uninitialize();
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}
void LogManagerPimpl::wakeup_loggers() {
  for (LoggerRef& logger : logger_refs_) {
    logger.wakeup();
  }
}

ErrorStack LogManagerPimpl::refresh_global_durable_epoch() {
  assorted::memory_fence_acquire();
  Epoch min_durable_epoch;
  ASSERT_ND(!min_durable_epoch.is_valid());
  for (const LoggerRef& logger : logger_refs_) {
    min_durable_epoch.store_min(logger.get_durable_epoch());
  }
  ASSERT_ND(min_durable_epoch.is_valid());

  if (min_durable_epoch <= get_durable_global_epoch()) {
    VLOG(0) << "durable_global_epoch_ not advanced";
    return kRetOk;
  }

  VLOG(0) << "Global durable epoch is about to advance from " << get_durable_global_epoch()
    << " to " << min_durable_epoch;
  {
    soc::SharedMutexScope guard(&control_block_->durable_global_epoch_savepoint_mutex_);
    if (min_durable_epoch <= get_durable_global_epoch()) {
      VLOG(0) << "oh, I lost the race.";
      return kRetOk;
    }

    CHECK_ERROR(engine_->get_savepoint_manager().take_savepoint(min_durable_epoch));

    // set durable_global_epoch_ within the SharedCond's mutex scope, and then broadcast.
    // this is required to avoid lost signals.
    soc::SharedMutexScope cond_guard(control_block_->durable_global_epoch_advanced_.get_mutex());
    control_block_->durable_global_epoch_ = min_durable_epoch.value();
    control_block_->durable_global_epoch_advanced_.broadcast(&cond_guard);
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
    for (LoggerRef& logger : logger_refs_) {
      logger.wakeup_for_durable_epoch(commit_epoch);
    }

    VLOG(0) << "Synchronously waiting for commit_epoch " << commit_epoch;
    if (wait_microseconds <= 0)  {
      // set durable_global_epoch_ within the SharedCond's mutex scope, and then wait.
      // this is required to avoid lost signals.
      soc::SharedMutexScope wait_guard(control_block_->durable_global_epoch_advanced_.get_mutex());
      if (commit_epoch <= get_durable_global_epoch()) {
        break;
      }
      control_block_->durable_global_epoch_advanced_.wait(&wait_guard);
      continue;
    }

    if (std::chrono::high_resolution_clock::now() >= until) {
      LOG(WARNING) << "Timeout occurs. wait_microseconds=" << wait_microseconds;
      return kErrorCodeTimeout;
    }

    {
      soc::SharedMutexScope wait_guard(control_block_->durable_global_epoch_advanced_.get_mutex());
      if (commit_epoch <= get_durable_global_epoch()) {
        break;
      }
      control_block_->durable_global_epoch_advanced_.timedwait(
        &wait_guard,
        wait_microseconds * 1000ULL);  // a bit lazy. we sleep longer in case of spurrious wakeup
    }
  }

  VLOG(0) << "durable epoch advanced. durable_global_epoch_=" << get_durable_global_epoch();
  return kErrorCodeOk;
}
void LogManagerPimpl::announce_new_durable_global_epoch(Epoch new_epoch) {
  ASSERT_ND(new_epoch >= Epoch(control_block_->durable_global_epoch_));
  soc::SharedMutexScope scope(control_block_->durable_global_epoch_advanced_.get_mutex());
  control_block_->durable_global_epoch_ = new_epoch.value();
  control_block_->durable_global_epoch_advanced_.broadcast(&scope);
}


void LogManagerPimpl::copy_logger_states(savepoint::Savepoint* new_savepoint) {
  new_savepoint->oldest_log_files_.clear();
  new_savepoint->oldest_log_files_offset_begin_.clear();
  new_savepoint->current_log_files_.clear();
  new_savepoint->current_log_files_offset_durable_.clear();
  for (const LoggerRef& logger : logger_refs_) {
    logger.copy_logger_state(new_savepoint);
  }
}

}  // namespace log
}  // namespace foedus
