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
MapReduceBase::MapReduceBase(Engine* engine, uint16_t id)
  : engine_(engine), parent_(engine), id_(id), numa_node_(engine->get_soc_id()), running_(false) {}

void MapReduceBase::launch_thread() {
  LOG(INFO) << "Launching thread for " << to_string();
  thread_ = std::move(std::thread(&MapReduceBase::handle, this));
}

void MapReduceBase::join_thread() {
  LOG(INFO) << "Waiting for the completion of thread: " << to_string();
  if (thread_.joinable()) {
    thread_.join();
  }
  LOG(INFO) << "Observed completion of thread: " << to_string();
}


ErrorCode MapReduceBase::check_cancelled() const {
  if (parent_.is_error()) {
    return kErrorCodeSnapshotCancelled;
  }
  return kErrorCodeOk;
}

void MapReduceBase::handle() {
  if (running_) {
    LOG(FATAL) << "Duplicate launch of " << to_string();
  }
  running_ = true;
  LOG(INFO) << "Started running: " << to_string() << " NUMA node=" << static_cast<int>(numa_node_);
  thread::NumaThreadScope scope(numa_node_);
  ErrorStack result = handle_process();  // calls main logic in derived class
  if (result.is_error()) {
    if (result.get_error_code() == kErrorCodeSnapshotCancelled) {
      LOG(WARNING) << to_string() << " cancelled";
    } else {
      LOG(ERROR) << to_string() << " got an error while processing:" << result;
      parent_.increment_error_count();
      parent_.wakeup();
    }
  } else {
    LOG(INFO) << to_string() << " successfully finished";
  }

  // let the gleaner know that I'm done.
  uint16_t value_after = parent_.increment_completed_count();
  ASSERT_ND(value_after <= parent_.get_all_count());
  if (value_after == parent_.get_all_count()) {
    // I was the last one to go into sleep, this means everything is fully processed.
    // let gleaner knows about it.
    ASSERT_ND(parent_.is_all_completed());
    LOG(INFO) << to_string() << " was the last one to finish, waking up gleaner.. ";
    parent_.wakeup();
  }

  parent_.increment_exit_count();
  LOG(INFO) << "Stopped running: " << to_string();
  running_ = false;
}

}  // namespace snapshot
}  // namespace foedus
