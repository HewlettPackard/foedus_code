/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/mapreduce_base_impl.hpp"

#include <glog/logging.h>

#include <chrono>
#include <ostream>
#include <sstream>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/snapshot/log_gleaner_impl.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/thread/numa_thread_scope.hpp"

namespace foedus {
namespace snapshot {

ErrorStack MapReduceBase::initialize_once() {
  LOG(INFO) << "Initializing " << to_string();
  // most of the initialization happens on its own thread (handle)
  thread_.initialize(to_string(),
          std::thread(&MapReduceBase::handle, this), std::chrono::milliseconds(10));
  return kRetOk;
}

ErrorStack MapReduceBase::uninitialize_once() {
  LOG(INFO) << "Uninitializing " << to_string();
  ErrorStackBatch batch;
  // most of the uninitialization happens on its own thread (handle), but we do it again
  // here in case there was some error.
  LOG(INFO) << "Calling handle_uninitialize at uninitialize_once: " << to_string() << "...";
  batch.emprace_back(handle_uninitialize());
  thread_.stop();
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorCode MapReduceBase::check_cancelled() {
  if (parent_->is_stop_requested()) {
    return kErrorCodeSnapshotCancelled;
  }
  return kErrorCodeOk;
}

void MapReduceBase::handle() {
  LOG(INFO) << "Reducer started running: " << to_string()
    << " NUMA node=" << static_cast<int>(numa_node_);
  thread::NumaThreadScope scope(numa_node_);

  LOG(INFO) << "Calling handle_initialize at handle(): " << to_string() << "...";
  ErrorStack init_error = handle_initialize();
  bool already_reported_error = false;
  if (init_error.is_error()) {
    LOG(ERROR) << to_string() << " failed to initialize:" << init_error;
    parent_->increment_error_count();
    already_reported_error = true;
    parent_->wakeup();
  } else {
    LOG(INFO) << to_string() << " initialization done";
    uint16_t value_after = parent_->increment_ready_to_start_count();
    if (value_after >= parent_->get_all_count()) {
      ASSERT_ND(parent_->is_all_ready_to_start());
      LOG(INFO) << to_string() << " was the last one to finish initialization";
      // let's wake up gleaner which is waiting for us.
      parent_->wakeup();
    }

    parent_->wait_for_start();
    ErrorStack result = handle_process();  // calls main logic in derived class
    if (result.is_error()) {
      if (result.get_error_code() == kErrorCodeSnapshotCancelled) {
        LOG(WARNING) << to_string() << " cancelled";
      } else {
        LOG(ERROR) << to_string() << " got an error while processing:" << result;
        parent_->increment_error_count();
        already_reported_error = true;
        parent_->wakeup();
      }
    } else {
      LOG(INFO) << to_string() << " successfully finished";
    }
  }

  handle_complete();

  LOG(INFO) << "Calling handle_uninitialize at handle(): " << to_string() << "...";
  ErrorStack uninit_error = handle_uninitialize();
  if (uninit_error.is_error()) {
    // error while uninitialize doesn't change what's happening. anyway the gleaner is dying.
    LOG(ERROR) << to_string() << " failed to uninitialize:" << uninit_error;
    if (!already_reported_error) {
      parent_->increment_error_count();
    }
  }

  parent_->increment_exit_count();
  LOG(INFO) << "Reducer stopped running: " << to_string();
}

void MapReduceBase::handle_complete() {
  pre_handle_uninitialize();

  // let the gleaner know that I'm done for the current epoch and going into sleep.
  uint16_t value_after = parent_->increment_completed_count();
  ASSERT_ND(value_after <= parent_->get_all_count());
  if (value_after == parent_->get_all_count()) {
    // I was the last one to go into sleep, this means everything is fully processed.
    // let gleaner knows about it.
    ASSERT_ND(parent_->is_all_completed());
    LOG(INFO) << to_string() << " was the last one to finish, waking up gleaner.. ";
    parent_->wakeup();
  }

  LOG(INFO) << to_string() << " Going into sleep...";
  parent_->wait_for_complete();
}


}  // namespace snapshot
}  // namespace foedus
