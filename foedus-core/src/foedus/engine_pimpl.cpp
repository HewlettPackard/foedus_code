/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/engine_pimpl.hpp"

#include <unistd.h>
#include <glog/logging.h>

#include <algorithm>
#include <string>

#include "foedus/error_stack_batch.hpp"

namespace foedus {

EnginePimpl::EnginePimpl(Engine* engine, const EngineOptions &options) :
  options_(options),
  engine_(engine),
  type_(kMaster),
  master_upid_(::getpid()),
  soc_id_(0),
  // although we give a pointer to engine, these objects must not access it yet.
  // even the Engine object has not set the pimpl pointer.
  soc_(engine),
  debug_(engine),
  memory_manager_(engine),
  savepoint_manager_(engine),
  thread_pool_(engine),
  log_manager_(engine),
  snapshot_manager_(engine),
  storage_manager_(engine),
  xct_manager_(engine),
  restart_manager_(engine) {
}
EnginePimpl::EnginePimpl(
  Engine* engine,
  EngineType type,
  uint64_t master_upid,
  uint16_t soc_id) :
  engine_(engine),
  type_(type),
  master_upid_(master_upid),
  soc_id_(soc_id),
  soc_(engine),
  debug_(engine),
  memory_manager_(engine),
  savepoint_manager_(engine),
  thread_pool_(engine),
  log_manager_(engine),
  snapshot_manager_(engine),
  storage_manager_(engine),
  xct_manager_(engine),
  restart_manager_(engine) {
}

std::string EnginePimpl::describe_short() const {
  if (type_ == kMaster) {
    return "MASTER";
  }
  std::string ret("CHILD-");
  return ret + std::to_string(soc_id_);
}

ErrorStack EnginePimpl::initialize_once() {
  for (Initializable* child : get_children()) {
    CHECK_ERROR(child->initialize());
  }
  LOG(INFO) << "================================================================================";
  LOG(INFO) << "================== FOEDUS ENGINE ("
    << describe_short() << ") INITIALIZATION DONE ===========";
  LOG(INFO) << "================================================================================";
  return kRetOk;
}
ErrorStack EnginePimpl::uninitialize_once() {
  LOG(INFO) << "================================================================================";
  LOG(INFO) << "=================== FOEDUS ENGINE ("
    << describe_short() << ") EXITTING...... ================";
  LOG(INFO) << "================================================================================";
  ErrorStackBatch batch;
  // uninit in reverse order of initialization
  auto children = get_children();
  std::reverse(children.begin(), children.end());
  for (Initializable* child : children) {
    CHECK_ERROR(child->uninitialize());
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}
}  // namespace foedus
